#include "AsyncRedisClusterNetClient.h"

#include <sys/prctl.h>

#include <string>
#include <utility>

#include "CLog.h"
#include "CMessageCollector.h"
#include "ReportServer.h"

// void RedisConnectCallBack(const redisClusterAsyncContext* c, int status) {
//   if (status != REDIS_OK) {
//     ERR_LOG("redisAsyncConnectCallback failed\n");
//   } else {
//     INFO_LOG("redisAsyncConnectCallback success\n");
//   }
// }

// void RedisDisconnectCallBack(const redisClusterAsyncContext* c, int status) {
//   if (status != REDIS_OK) {
//     ERR_LOG("redisAsyncDisconnectCallback failed\n");
//   } else {
//     INFO_LOG("redisAsyncDisconnectCallback success\n");
//   }
// }

void RedisHandleCallBack(redisClusterAsyncContext* c, void* r, void* privdata) {
  DEBUG_LOG("redis call back ~~~");
  if (privdata == NULL) return;

  RedisAsyncContext* data = reinterpret_cast<RedisAsyncContext*>(privdata);
  if (c != NULL && r != NULL) {
    string info;
    redisReply* reply = reinterpret_cast<redisReply*>(r);
    AsyncRedisClusterNetClient* client =
        reinterpret_cast<AsyncRedisClusterNetClient*>(data->get_client());
    auto rsp = data->get_redis_rsp();
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
      client->Handle(&rsp, NULL, 1);
    }
    if (reply->str) {
      client->Handle(&rsp, reply->str, reply->len);
    } else {
      client->Handle(&rsp, NULL, 0);
    }
  }
  DELETE(data);
}

AsyncRedisClusterNetClient::~AsyncRedisClusterNetClient() {
  ERR_LOG("destruct redis cluster async netclient: %s", m_GroupName.c_str());
  for (const auto& thread : handle_thread_pool_) {
    pthread_join(thread->getThreadID(), NULL);
  }

  for (const auto& thread : queue_thread_pool_) {
    pthread_join(thread->getThreadID(), NULL);
  }

  event_base_loopbreak(event_loop_);
}

bool AsyncRedisClusterNetClient::Init(SyncRedisClusterModuleConfig& config) {
  m_GroupName = config.groupname;
  DEBUG_LOG("redis cluster async init netclient: %s", m_GroupName.c_str());

  for (int i = 0; i < config.threadnum; i++) {
    event_loop_ = event_base_new();
    AsyncRedisClusterAccess::ptr access = std::make_shared<AsyncRedisClusterAccess>();
    if (!access) {
      ERR_LOG("init async redis cluster access instance failed, redis cluster:%s",
              config.address.c_str());
      return false;
    }
    return_info info = access->async_connect(event_loop_, config.address, config.timeout);
    if (info.code != NO_ERROR) {
      ERR_LOG("connect async redis cluster failed, redis cluster:%s， errocde:%d",
              config.address.c_str(), info.code);
      return false;
    }
    DEBUG_LOG("init event_loop_[%p]", event_loop_);
    // handle
    AsyncRedisClusterHandleThread::ptr handle_thread =
        std::make_shared<AsyncRedisClusterHandleThread>(event_loop_);
    if (!handle_thread) {
      access->async_disconnect();
      ERR_LOG("init async redis cluster handle thread instance failed");
      return false;
    }
    handle_thread_pool_.push_back(handle_thread);
    handle_thread->start();
    // queue
    AsyncRedisClusterProcessThread::ptr process_thread =
        std::make_shared<AsyncRedisClusterProcessThread>(access, this, event_loop_);
    if (!process_thread) {
      access->async_disconnect();
      ERR_LOG("init async redis cluster process thread instance failed");
      return false;
    }
    queue_thread_pool_.push_back(process_thread);
    process_thread->start();
    usleep(1000);
  }

  return true;
}

int AsyncRedisClusterHandleThread::run() {
  // 事件监听
  // if (event_loop_ != NULL)
  //   event_base_dispatch(event_loop_);
  // else
  //   DEBUG_LOG("AsyncRedisClusterHandleThread event_loop_ is null");
  // event_base_free(event_loop_);
  // ERR_LOG("AsyncRedisClusterHandleThread end");

  while (m_threadstate) {
    int ret = event_base_loop(event_loop_, EVLOOP_NO_EXIT_ON_EMPTY);
    if (ret == 1)
      continue;
    else if (ret == 0)
      break;
    else
      ERR_LOG("workThread event base %p event_base_loop return %d", event_loop_, ret);
  }
  event_base_free(event_loop_);
  return 0;
}

int AsyncRedisClusterProcessThread::run() {
  // 事件监听
  while (m_threadstate) {
    CCommQuery* query = net_client_->GetMessage();
    if (NULL != query) {
      DEBUG_LOG("get message CCommQuery[%u,%u]  to Process", query->m_sessionid,
                query->m_messageid);
      // std::lock_guard<std::mutex> lck(mx_);
      net_client_->ExecCommand(redis_access_, query);
    } else {
      ERR_LOG("get message is null");
    }
  }
  return 0;
}

bool AsyncRedisClusterNetClient::SendMessage(CDataProxySessionMsg* session, CCommQuery* query) {
  CCommQuery* copyquery = NULL;
  NEW(copyquery, CCommQuery);
  if (NULL == copyquery) {
    ERR_LOG("CCommQuery[%u,%u] SendMessage Copy  Query Failed!!!", query->m_sessionid,
            query->m_messageid);
    return false;
  }
  *copyquery = *query;
  DEBUG_LOG("send message start");
  if (queue_.postmsg(copyquery) == false) {
    ERR_LOG("CCommQuery[%u,%u] Enqueue Failed, Queue is Full", query->m_sessionid,
            query->m_messageid);
    DELETE(copyquery);
    return false;
  }

  return true;
}

CCommQuery* AsyncRedisClusterNetClient::GetMessage() { return queue_.getmsg(); }

void AsyncRedisClusterNetClient::stop() {
  for (const auto& thread : handle_thread_pool_) {
    thread->stop();
    pthread_cancel(thread->getThreadID());
  }

  for (const auto& thread : queue_thread_pool_) {
    thread->stop();
    pthread_cancel(thread->getThreadID());
  }
}

bool AsyncRedisClusterNetClient::ExecCommand(AsyncRedisClusterAccess::ptr ars, CCommQuery* query) {
  // check if keyVec size in redis query is 0
  if (!query->m_Query.m_KeyVec.size()) {
    ERR_LOG("RedisAsyncClusterNetClient SendMessage KeyVector Size is 0");
    return false;
  }

  RedisRspInfo rspinfo;
  rspinfo.querytime = CPsudoTime::getInstance()->getCurrentMSec();
  rspinfo.query = query;

  RedisAsyncContext* async_data = NULL;
  NEW(async_data, RedisAsyncContext(this, &rspinfo));
  if (async_data == NULL) {
    ERR_LOG("Create redis async data error");
    return false;
  }
  DEBUG_LOG("ExecCommand Get CCommQuery[%u,%u]  to Process", async_data->rsp.query->m_sessionid,
            async_data->rsp.query->m_messageid);

  if (1 < query->m_Query.m_KeyVec.size()) {
    ERR_LOG("Not Support here now");
    DELETE(async_data);
    return false;
  } else if (query->m_Query.m_KeyVec.size() == 1) {
    // async get
    std::string query_key = query->m_Query.m_KeyVec[0];
    if (query->m_Query.m_Cmd == "get") {
      int reply_ret = ars->get(query_key.c_str(), RedisHandleCallBack, async_data);
      if (reply_ret != 0) {
        Handle(&rspinfo, NULL, 1);
        DELETE(async_data);
        return false;
      }
    } else {
      Handle(&rspinfo, NULL, 1);
      DELETE(async_data);
      return false;
    }
  }

  return true;
}

void AsyncRedisClusterNetClient::Handle(RedisRspInfo* rspinfo, const char* buf, unsigned int len) {
  CCommReply* reply = NULL;
  NEW(reply, CCommReply);
  if (NULL == reply) {
    ERR_LOG("RedisAsyncClusterNetClient ProcessThread:CCommQuery[%u,%u] New CCommReply Failed",
            rspinfo->query->m_sessionid, rspinfo->query->m_messageid);
    return;
  }
  DEBUG_LOG("RedisAsyncClusterNetClient hand CCommQuery[%u,%u] len[%u] to Process",
            rspinfo->query->m_sessionid, rspinfo->query->m_messageid, len);

  uint64_t nowtime = CPsudoTime::getInstance()->getCurrentMSec();
  uint64_t costtime = nowtime - rspinfo->querytime;
  DEBUG_LOG("redis query cost %llu", costtime);

  CCommQuery* query;
  query = rspinfo->query;

  if (costtime > 10) {
    // TDESyncReq_LocalStatic(m_GroupName, query->m_Tableid, TDECMDTYPE_GET, ResultType_TIMEOUT, 0);
    INFO_LOG("redis query cost %llu", costtime);
  }

  reply->m_reply.m_ResultCode = STATE_KEYVALUE_NORAML;
  if (buf == NULL) {
    if (len == 0) {
      reply->m_reply.m_ResultCode = STATE_KEYVALUE_CAN_NOT_FIND_KEY;
    } else {
      reply->m_reply.m_ResultCode = STATE_KEYVALUE_SYS_ERROR;
    }
  }
  reply->m_reply.m_arg = query->m_Query.m_Args;
  if (buf == NULL) {
    reply->m_reply.m_KeyValueMap.insert(make_pair(query->m_Query.m_KeyVec[0], ""));
  } else {
    string rep = "";
    rep.append(buf, len);
    reply->m_reply.m_KeyValueMap.insert(make_pair(query->m_Query.m_KeyVec[0], rep));
  }
  reply->m_sessionid = query->m_sessionid;
  reply->m_messageid = query->m_messageid;
  reply->m_StrTableName = query->m_StrTableName;
  reply->m_Tableid = query->m_Tableid;
  reply->m_reply.m_TableName = query->m_Query.m_TableName;
  CMessageCollector::GetInstance()->SetResponseMessage(reply);
  DELETE(query);
}
