#include "NetClient.h"

#include <string>

#include "AsyncRedisClusterNetClient.h"
#include "CConfigReader.h"
#include "CHoldReqMessage.h"
#include "CLog.h"
#include "CMessageCollector.h"
#include "DataProxy.h"
#include "LocalStatLog.h"
#include "PegasusAsyncClient.h"
#include "StringTool.h"
#include "SyncRedisClusterNetClient.h"
#include "TairNetClient.h"
#include "redis_hash.pb.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "event2/thread.h"
#ifdef __cplusplus
}
#endif

using namespace extend_so;

const char* const NetClientMgn::s_tairLogLevelStr[] = {"DEBUG", "INFO", "WARN", "ERROR"};
extern DataProxy* GetDataProxy();

bool NetClientMgn::init() {
  if (m_tairAsyncConfig.size()) {
    TBSYS_LOGGER.setLogLevel(s_tairLogLevelStr[CConfigReader::GetTdeLog().loglevel]);
    string logpath = CConfigReader::GetTdeLog().logpath;
    char tmp[10];
    snprintf(tmp, sizeof(tmp), "_%d", getpid());
    logpath = logpath + tmp;
    TBSYS_LOGGER.setFileName(logpath.c_str());
    TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 512);
  }
  async_flag_ = CConfigReader::get_async_flag();
  
  InitTairAsyncNetClient(m_tairAsyncConfig, m_onlineNetClient);
  InitRedisClusterAsyncNetClient(m_redisClusterSyncConfig, m_onlineNetClient);
  InitAsyncRedisNetClient(async_redis_config_, m_onlineNetClient);
  InitPegasusAsyncNetClient(m_pegasusAsyncConfig, m_onlineNetClient);

  return true;
}

int NetClientMgn::LoadTairAsyncConfig(CSelectResult& result, CNewMysql* mysql, char* err) {
  char query[4096];
  snprintf(query, 4095, "select * from TairAsync where modifytime > '%s' and now() > modifytime+2",
           m_tairAsyncConfigLastUpdateTime.c_str());
  int ret = mysql->query(query, result);
  if (0 != ret) {
    snprintf(err, 1024, "NetClientMgn initConfig TairAsync MySql Query Failed!!!! ret:%d reason:%s",
             ret, mysql->getErr());
    return ERR_SQL_QUERY_FAILED;
  }

  return 0;
}

bool NetClientMgn::SetTairAsyncResult(
    CSelectResult& result, std::map<std::string, TdeAsyncModuleConfig>& freshTairAsyncConfig,
    char* err) {
  int32_t rownum = result.num_rows();
  if (rownum <= 0) {
    return true;
  }

  std::string lastmodifytime = result[0]["modifytime"];
  for (int32_t idx = 0; idx < rownum; ++idx) {
    // mysql建表语句中设置所有字段不能为空，所以这里不做空异常判断
    TdeAsyncModuleConfig tairAsyncConfig;
    tairAsyncConfig.groupname = result[idx]["GroupName"];
    tairAsyncConfig.masterConfigServer = result[idx]["Master"];
    tairAsyncConfig.slaveConfigServer = result[idx]["Slave"];
    tairAsyncConfig.threadnum = atoi(result[idx]["ThreadNum"]);
    tairAsyncConfig.timeout = atoi(result[idx]["TimeOut"]);
    tairAsyncConfig.status = atoi(result[idx]["Status"]);

    if (lastmodifytime < result[idx]["modifytime"]) {
      lastmodifytime = result[idx]["modifytime"];
    }

    freshTairAsyncConfig[tairAsyncConfig.groupname] = tairAsyncConfig;
    ERR_LOG("load tair async config: %s", tairAsyncConfig.ToString().c_str());
  }
  m_tairAsyncConfigLastUpdateTime = lastmodifytime;

  return true;
}

int NetClientMgn::LoadRedisClusterSyncConfig(CSelectResult& result, CNewMysql* mysql, char* err) {
  char query[4096];
  snprintf(query, 4095,
           "select * from RedisClusterSync where modifytime > '%s' and now() > modifytime+2",
           m_redisClusterSyncConfigLastUpdateTime.c_str());
  int ret = mysql->query(query, result);
  if (0 != ret) {
    snprintf(err, 1024,
             "NetClientMgn initConfig RedisClusterSync MySql Query Failed!!!! ret:%d reason:%s",
             ret, mysql->getErr());
    return ERR_SQL_QUERY_FAILED;
  }

  return 0;
}

bool NetClientMgn::SetRedisClusterSyncResult(
    CSelectResult& result,
    std::map<std::string, SyncRedisClusterModuleConfig>& freshRedisClusterSyncConfig, char* err) {
  int32_t rownum = result.num_rows();
  if (rownum <= 0) {
    return true;
  }

  std::string lastmodifytime = result[0]["modifytime"];
  for (int32_t idx = 0; idx < rownum; ++idx) {
    // mysql建表语句中设置所有字段不能为空，所以这里不做空异常判断
    SyncRedisClusterModuleConfig redisClusterSyncConfig;
    redisClusterSyncConfig.groupname = result[idx]["GroupName"];
    redisClusterSyncConfig.address = result[idx]["Address"];
    redisClusterSyncConfig.threadnum = atoi(result[idx]["ThreadNum"]);
    redisClusterSyncConfig.timeout = atoi(result[idx]["TimeOut"]);
    redisClusterSyncConfig.redis_version = atoi(result[idx]["Version"]);
    redisClusterSyncConfig.status = atoi(result[idx]["Status"]);

    if (lastmodifytime < result[idx]["modifytime"]) {
      lastmodifytime = result[idx]["modifytime"];
    }

    freshRedisClusterSyncConfig[redisClusterSyncConfig.groupname] = redisClusterSyncConfig;
    ERR_LOG("load redis cluster sync config: %s", redisClusterSyncConfig.ToString().c_str());
  }

  m_redisClusterSyncConfigLastUpdateTime = lastmodifytime;
  return true;
}

int NetClientMgn::LoadPegasusAsyncConfig(CSelectResult& result, CNewMysql* mysql, char* err) {
  char query[4096];
  snprintf(query, 4095,
           "select * from PegasusAsync where modifytime > '%s' and now() > modifytime+2",
           m_pegasusAsyncConfigLastUpdateTime.c_str());
  int ret = mysql->query(query, result);
  if (0 != ret) {
    snprintf(err, 1024,
             "NetClientMgn initConfig PegasusAsync MySql Query Failed!!!! ret:%d reason:%s", ret,
             mysql->getErr());
    return ERR_SQL_QUERY_FAILED;
  }

  return 0;
}

bool NetClientMgn::SetPegasusAsyncConfig(
    CSelectResult& result, std::map<std::string, PegasusAsyncModuleConfig>& freshPegasusAsyncConfig,
    char* err) {
  int32_t rownum = result.num_rows();
  if (rownum <= 0) {
    return true;
  }

  std::string lastmodifytime = result[0]["modifytime"];
  for (int32_t idx = 0; idx < rownum; ++idx) {
    PegasusAsyncModuleConfig pegasusAsyncConfig;
    pegasusAsyncConfig.groupname = result[idx]["GroupName"];
    pegasusAsyncConfig.address = result[idx]["Address"];
    pegasusAsyncConfig.status = atoi(result[idx]["Status"]);

    if (lastmodifytime < result[idx]["modifytime"]) {
      lastmodifytime = result[idx]["modifytime"];
    }

    freshPegasusAsyncConfig[pegasusAsyncConfig.groupname] = pegasusAsyncConfig;
    ERR_LOG("load pegasus async config: %s", pegasusAsyncConfig.ToString().c_str());
  }

  m_pegasusAsyncConfigLastUpdateTime = lastmodifytime;
  return true;
}

int NetClientMgn::initConfig(char* err, CNewMysql* mysql) {
  CSelectResult tairAsyncResult;
  LoadTairAsyncConfig(tairAsyncResult, mysql, err);
  SetTairAsyncResult(tairAsyncResult, m_tairAsyncConfig, err);

  CSelectResult redisClusterSyncResult;
  LoadRedisClusterSyncConfig(redisClusterSyncResult, mysql, err);
  SetRedisClusterSyncResult(redisClusterSyncResult, m_redisClusterSyncConfig, err);
  SetRedisClusterSyncResult(redisClusterSyncResult, async_redis_config_, err);

  CSelectResult pegasusAsyncResult;
  LoadPegasusAsyncConfig(pegasusAsyncResult, mysql, err);
  SetPegasusAsyncConfig(pegasusAsyncResult, m_pegasusAsyncConfig, err);

  return 0;
}

int NetClientMgn::RefreshConfig(char* err, CNewMysql* mysql) {
  CSelectResult tairAsyncResult;
  LoadTairAsyncConfig(tairAsyncResult, mysql, err);
  std::map<std::string, TdeAsyncModuleConfig> freshTairAsyncConfig;
  std::map<std::string, std::shared_ptr<NetClient>> freshTairAsyncNetClient;
  SetTairAsyncResult(tairAsyncResult, freshTairAsyncConfig, err);
  if (freshTairAsyncConfig.size()) {
    InitTairAsyncNetClient(freshTairAsyncConfig, freshTairAsyncNetClient);
  }

  CSelectResult redisClusterSyncResult;
  LoadRedisClusterSyncConfig(redisClusterSyncResult, mysql, err);
  std::map<std::string, SyncRedisClusterModuleConfig> freshRedisClusterConfig;
  std::map<std::string, std::shared_ptr<NetClient>> freshRedisClusterSyncNetClient;
  std::map<std::string, std::shared_ptr<NetClient>> fresh_async_redis_netclient;
  SetRedisClusterSyncResult(redisClusterSyncResult, freshRedisClusterConfig, err);
  if (freshRedisClusterConfig.size()) {
    InitRedisClusterAsyncNetClient(freshRedisClusterConfig, freshRedisClusterSyncNetClient);
    InitAsyncRedisNetClient(freshRedisClusterConfig, fresh_async_redis_netclient);
  }

  CSelectResult pegasusAsyncResult;
  LoadPegasusAsyncConfig(pegasusAsyncResult, mysql, err);
  std::map<std::string, PegasusAsyncModuleConfig> freshPegasusAsyncConfig;
  std::map<std::string, std::shared_ptr<NetClient>> freshPegasusAsyncNetClient;
  SetPegasusAsyncConfig(pegasusAsyncResult, freshPegasusAsyncConfig, err);
  if (freshPegasusAsyncConfig.size()) {
    InitPegasusAsyncNetClient(freshPegasusAsyncConfig, freshPegasusAsyncNetClient);
  }

  time_t nowTime = time(NULL);
  do {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (freshTairAsyncNetClient.size()) {
      for (const auto& client : freshTairAsyncNetClient) {
        auto oldClient = m_onlineNetClient.find(client.first);
        if (oldClient != m_onlineNetClient.end()) {
          m_deferDeleteNetClient[nowTime].push_back(oldClient->second);
        }
        m_onlineNetClient[client.first] = client.second;
        ERR_LOG("refresh tair async net client: %s", client.first.c_str());
      }
    }
    if (freshRedisClusterSyncNetClient.size()) {
      for (const auto& client : freshRedisClusterSyncNetClient) {
        auto oldClient = m_onlineNetClient.find(client.first);
        if (oldClient != m_onlineNetClient.end()) {
          m_deferDeleteNetClient[nowTime].push_back(oldClient->second);
        }
        m_onlineNetClient[client.first] = client.second;
        ERR_LOG("refresh redis cluster sync net client: %s", client.first.c_str());
      }
    }
    if (fresh_async_redis_netclient.size()) {
      for (const auto& client : fresh_async_redis_netclient) {
        std::string key = client.first + "_async";
        auto oldClient = m_onlineNetClient.find(key);
        if (oldClient != m_onlineNetClient.end()) {
          m_deferDeleteNetClient[nowTime].push_back(oldClient->second);
        }
        m_onlineNetClient[key] = client.second;
        ERR_LOG("refresh redis cluster async net client: %s", key.c_str());
      }
    }
    if (freshPegasusAsyncNetClient.size()) {
      for (const auto& client : freshPegasusAsyncNetClient) {
        auto oldClient = m_onlineNetClient.find(client.first);
        if (oldClient != m_onlineNetClient.end()) {
          m_deferDeleteNetClient[nowTime].push_back(oldClient->second);
        }
        m_onlineNetClient[client.first] = client.second;
        ERR_LOG("refresh pegasus async net client: %s", client.first.c_str());
      }
    }

    //下线中途下线的客户端，在mysql中将status字段置为0
    for (const auto& config : freshTairAsyncConfig) {
      if (config.second.status) {
        continue;
      }
      auto offlineClient = m_onlineNetClient.find(config.first);
      if (offlineClient != m_onlineNetClient.end()) {
        ERR_LOG("offline tair async netclient: %s", config.first.c_str());
        m_deferDeleteNetClient[nowTime].push_back(offlineClient->second);
        m_onlineNetClient.erase(offlineClient);
      }
    }
    for (const auto& config : freshRedisClusterConfig) {
      if (config.second.status) {
        continue;
      }
      auto offlineClient = m_onlineNetClient.find(config.first);
      if (offlineClient != m_onlineNetClient.end()) {
        ERR_LOG("offline redis cluster sync netclient: %s", config.first.c_str());
        m_deferDeleteNetClient[nowTime].push_back(offlineClient->second);
        m_onlineNetClient.erase(offlineClient);
      }
    }
    for (const auto& config : freshPegasusAsyncConfig) {
      if (config.second.status) {
        continue;
      }
      auto offlineClient = m_onlineNetClient.find(config.first);
      if (offlineClient != m_onlineNetClient.end()) {
        ERR_LOG("offline pegasus async netclient: %s", config.first.c_str());
        m_deferDeleteNetClient[nowTime].push_back(offlineClient->second);
        m_onlineNetClient.erase(offlineClient);
      }
    }
  } while (0);

  std::set<time_t> needDeleteSet;
  needDeleteSet.clear();
  for (const auto& client : m_deferDeleteNetClient) {
    if (client.first + 60 < nowTime) {
      needDeleteSet.insert(client.first);
    }
  }
  for (const auto& iter : needDeleteSet) {
    needDeleteSet.erase(iter);
  }

  return 0;
}

bool NetClientMgn::InitTairAsyncNetClient(
    std::map<std::string, TdeAsyncModuleConfig>& tairAsyncConfig,
    std::map<std::string, std::shared_ptr<NetClient>>& tairAsyncNetClient) {
  if (tairAsyncConfig.size() == 0) {
    return true;
  }

  for (auto config : tairAsyncConfig) {
    if (config.second.status == 0) {
      DEBUG_LOG("TairAsyncClient %s status is 0, ignore it", config.second.groupname.c_str());
      continue;
    }

    std::shared_ptr<TairAsyncNetClient> newTairAsyncNetClient =
        std::make_shared<TairAsyncNetClient>();

    if (false == newTairAsyncNetClient->init(config.second)) {
      ERR_LOG("NetClientMgn Create TairAsyncNetClient[%s] failed", config.first.c_str());
      continue;
    }

    tairAsyncNetClient[config.first] = newTairAsyncNetClient;
    ERR_LOG("NetClientMgn Create TairAsyncNetClient[%s] success", config.first.c_str());
  }

  return true;
}

bool NetClientMgn::InitAsyncRedisNetClient(
    std::map<std::string, SyncRedisClusterModuleConfig>& redis_config,
    std::map<std::string, std::shared_ptr<NetClient>>& ptr_net_client) {
  if (!async_flag_) return true;
  DEBUG_LOG("redis_config size:%zd", redis_config.size());
  if (redis_config.size() == 0) {
    return true;
  }
  for (auto config : redis_config) {
    if (config.second.status == 0) {
      DEBUG_LOG("ptr_net_client %s status is 0, ignore it", config.second.groupname.c_str());
      continue;
    }

    std::shared_ptr<AsyncRedisClusterNetClient> net_client =
        std::make_shared<AsyncRedisClusterNetClient>();
    DEBUG_LOG("async redis init");
    if (false == net_client->Init(config.second)) {
      ERR_LOG("NetClientMgn Create ptr_net_client[%s] failed", config.first.c_str());
      continue;
    }
    std::string key = config.first + "_async";
    ptr_net_client[key] = net_client;
    ERR_LOG("NetClientMgn Create ptr_net_client[%s] success", key.c_str());
  }

  return true;
}

bool NetClientMgn::InitRedisClusterAsyncNetClient(
    std::map<std::string, SyncRedisClusterModuleConfig>& redisClusterSyncConfig,
    std::map<std::string, std::shared_ptr<NetClient>>& redisClusterSyncNetClient) {
  if (async_flag_) return true;
  if (redisClusterSyncConfig.size() == 0) {
    return true;
  }
  for (auto config : redisClusterSyncConfig) {
    if (config.second.status == 0) {
      DEBUG_LOG("RedisClusterSyncNetClient %s status is 0, ignore it",
                config.second.groupname.c_str());
      continue;
    }

    std::shared_ptr<SyncRedisClusterNetClient> newRedisClusterSyncNetClient =
        std::make_shared<SyncRedisClusterNetClient>();

    if (false == newRedisClusterSyncNetClient->init(config.second)) {
      ERR_LOG("NetClientMgn Create RedisClusterSyncNetClient[%s] failed", config.first.c_str());
      continue;
    }

    redisClusterSyncNetClient[config.first] = newRedisClusterSyncNetClient;
    ERR_LOG("NetClientMgn Create RedisClusterSyncNetClient[%s] success", config.first.c_str());
  }

  return true;
}

bool NetClientMgn::InitPegasusAsyncNetClient(
    std::map<std::string, PegasusAsyncModuleConfig>& pegasusAsyncConfig,
    std::map<std::string, std::shared_ptr<NetClient>>& pegasusAsyncNetCLient) {
  if (pegasusAsyncConfig.size() == 0) {
    return true;
  }

  for (auto config : pegasusAsyncConfig) {
    if (config.second.status == 0) {
      DEBUG_LOG("PegasusAsyncNetClient %s status is 0, ignore it", config.second.groupname.c_str());
      continue;
    }

    std::shared_ptr<PegasusAsyncClient> newPegasusAsyncClient =
        std::make_shared<PegasusAsyncClient>();

    if (false == newPegasusAsyncClient->init(config.first)) {
      ERR_LOG("NetClientMgn Create PegasusAsyncClient[%s] failed", config.first.c_str());
      continue;
    }

    pegasusAsyncNetCLient[config.first] = newPegasusAsyncClient;
    ERR_LOG("NetClientMgn Create PegasusAsyncClient[%s] success", config.first.c_str());
  }

  return true;
}

int32_t NetClientMgn::SendMessage(CDataProxySessionMsg* session, vector<ExtendSoQuery>& queryVec) {
  //当前dataproxy不支持multi操作，先简单来，以后有需要了在打开
  if (queryVec.size() != 1) {
    ERR_LOG("queryVec size[%d] is not equal 1, can not support currently", queryVec.size());
    return ERR_CANNOT_SUPPORT_MULTI_OPT;
  }

  //走到这里说明queryVec的大小是1
  atomic_inc(&(session->m_AsyncCounter));
  GetDataProxy()->GetSessionMgn()->saveSession(session);

  int32_t retCode = 0;
  CTGuard<CTMutex> guard(session->m_mutex);
  session->m_EnableState = true;
  DWORD id = 0;
  do {
    DEBUG_LOG("Session[%u] Send ExtendSoQuery:[TableName:%s,KeyNum:%d,Cmd:%s,ArgLen:%d]",
              session->m_seq, queryVec[0].m_TableName.c_str(), queryVec[0].m_KeyVec.size(),
              queryVec[0].m_Cmd.c_str(), queryVec[0].m_Args.length());

    TableStorageLocation location;
    if (false == GetDataProxy()->GetDataStorageMgn()->GetTableStorageLocation(
                     queryVec[0].m_TableName, location)) {
      ERR_LOG("NetClientMgn can not find the table:%s location", queryVec[0].m_TableName.c_str());
      retCode = ERR_SERVER_CAN_NOT_FIND_STORAGE_LOCATION;
      break;
    }
    DEBUG_LOG("VirtualTable:%s is TableStorageLocation:%s", queryVec[0].m_TableName.c_str(),
              location.toString().c_str());

    CCommQuery* pquery = NULL;
    NEW(pquery, CCommQuery);
    if (NULL == pquery) {
      ERR_LOG("Session[%u] Send ExtendSoQuery, NEW CCommQuery Failed!!!", session->m_seq);
      retCode = ERR_SERVER_NEW_EXTERD_SO_QUERY_FAILED;
      break;
    }

    pquery->m_sessionid = session->m_seq;
    pquery->m_messageid = id++;
    pquery->m_Tableid = location.m_tableID;
    pquery->m_ExpireTime = location.m_expireTime;
    pquery->m_StrTableName = queryVec[0].m_TableName;
    pquery->m_Query = queryVec[0];
    pquery->m_SessionTimeOutTime = session->m_TimeOutTime;
    std::string group_name = location.m_groupName;
    if (async_flag_) group_name += "_async";
    auto net_client = GetNetClient(group_name);
    if (net_client) {
      session->SetGroupName(location.m_groupName);
      if (net_client->SendMessage(session, pquery)) {
        DEBUG_LOG(
            "Session[%u]Message[%u] Send CCommQuery[Tableid:%u,TableName:%s,KeySize:%u] Success!!",
            pquery->m_sessionid, pquery->m_messageid, pquery->m_Tableid,
            pquery->m_StrTableName.c_str(), pquery->m_Query.m_KeyVec.size());
        session->AddQuery(pquery);
      } else {
        ERR_LOG("Session[%u]Message[%u] sendMessage Failed!!", pquery->m_sessionid,
                pquery->m_messageid);
        DELETE(pquery);
      }
    } else {
      ERR_LOG("Session[%u],Message[%u] can not find NetClient[%s]", session->m_seq,
              pquery->m_messageid, location.m_groupName.c_str());
      DELETE(pquery);
      retCode = ERR_SERVER_CAN_NOT_FIND_NETCLIENT;
      break;
    }
  } while (0);

  if (retCode) {
    atomic_dec(&(session->m_AsyncCounter));
    //因为session中有两个计数器，这里必须调用这个函数，否则会出现严重的bug
    GetDataProxy()->GetSessionMgn()->eraseAndGetSession(session->m_seq);
    session->release_this();
  }

  return retCode;
}

void NetClientMgn::stop() {
  for (const auto& client : m_onlineNetClient) {
    client.second->stop();
  }
}

void NetClientMgn::FiveMinuterTimer() {
  for (const auto& client : m_onlineNetClient) {
    client.second->FiveMinuterTimer();
  }
}
