#include "async_redis_cluster_access.h"

#include "CLog.h"
//#include "utils.h"
//#include <glog/logging.h>
#include <algorithm>

void AsyncRedisClusterAccess::redis_disconnect_callback(const struct redisAsyncContext* ctx,
                                                        int status) {
  ERR_LOG("async redis cluster disconnect......");
  if (!ctx || !ctx->data) {
    // LOG(ERROR) << "redis connection create error:ctx or ctx->data is null.";
    return;
  }
  AsyncRedisClusterAccess* p_access = (AsyncRedisClusterAccess*)ctx->data;
  if (p_access->redis_dog_tag != REDIS_DOG_TAG) {
    ERR_LOG("redis connection create error:user data is invalid async_redis_access");
  } else {
    p_access->conn_state = CONN_STATE_DOWN;
    if (status != REDIS_OK) {
      ERR_LOG("redis disconnect error[%s]", ctx->errstr);
    } else {
      ;  // LOG(INFO) << "redis disconnect.[ip=" << p_access->m_ip.c_str() << ",port=" <<
         // p_access->m_port << "].";
    }
    p_access->has_conn_err = true;
    if (p_access->m_redis_disconnect_cb) {
      p_access->m_redis_disconnect_cb(p_access, p_access->m_user_data);
    }
  }
}
void AsyncRedisClusterAccess::redis_connect_callback(const struct redisAsyncContext* ctx,
                                                     int status) {
  if (!ctx || !ctx->data) {
    DEBUG_LOG("redis connection create error:ctx or ctx->data is null.");
    return;
  }
  AsyncRedisClusterAccess* p_access = reinterpret_cast<AsyncRedisClusterAccess*>(ctx->data);
  if (p_access->redis_dog_tag != REDIS_DOG_TAG) {
    DEBUG_LOG("redis tag not dog_tag :%d, status:%d", p_access->redis_dog_tag, status);
    // redisClusterAsyncDisconnect(p_access->async_redis_ctx_);
    return;
  }
  if (status != REDIS_OK) {
    p_access->conn_state = CONN_STATE_DOWN;
    p_access->has_conn_err = true;
  } else {
    p_access->conn_state = CONN_STATE_ON;
    p_access->has_conn_err = false;
  }
  if (p_access->m_redis_connect_cb) {
    p_access->m_redis_connect_cb(p_access, p_access->m_user_data, !p_access->has_conn_err);
  }
  gettimeofday(&p_access->last_success_time, NULL);
  redisClusterAsyncCommand(p_access->async_redis_ctx_, redis_time_callback, p_access, "time");
}

void AsyncRedisClusterAccess::redis_ping_callback(redisClusterAsyncContext* ctx, void* r,
                                                  void* privdata) {
  DEBUG_LOG("call back ping !!!");
  if (privdata && ctx && r) {
    AsyncRedisClusterAccess* redis = (AsyncRedisClusterAccess*)privdata;
    string info;
    // parse result,then get cache data
    if (AsyncRedisClusterAccess::parse_reply((redisReply*)r, info) == 0) {
      if (strcmp(info.c_str(), "PONG") != 0) {
        redis->async_reconnect();
      } else {
        DEBUG_LOG("call back ping success!!!");
        gettimeofday(&redis->last_success_time, NULL);
      }
    }
  }
}

void AsyncRedisClusterAccess::redis_time_callback(redisClusterAsyncContext* ctx, void* r,
                                                  void* privdata) {
  if (privdata && ctx && r) {
    AsyncRedisClusterAccess* redis = (AsyncRedisClusterAccess*)privdata;
    string info;
    // parse result,then get cache data
    if (AsyncRedisClusterAccess::parse_reply((redisReply*)r, info) == 0) {
      if (info.length() > 0) {
        gettimeofday(&redis->last_success_time, NULL);
        long redis_time = redis->last_success_time.tv_sec;
        sscanf(info.c_str(), "%ld", &redis_time);
        redis->diff_local_time_seconds = redis_time - redis->last_success_time.tv_sec;
      }
    }
  }
}

void AsyncRedisClusterAccess::redis_get_callback(redisClusterAsyncContext* ctx, void* r,
                                                 void* privdata) {
  if (privdata && ctx && r) {
    AsyncRedisClusterAccess* redis = (AsyncRedisClusterAccess*)privdata;
    string info;
    // parse result,then get cache data
    if (AsyncRedisClusterAccess::parse_reply((redisReply*)r, info) == 0) {
      if (info.length() > 0) {
        gettimeofday(&redis->last_success_time, NULL);
        long redis_time = redis->last_success_time.tv_sec;
        sscanf(info.c_str(), "%ld", &redis_time);
        redis->diff_local_time_seconds = redis_time - redis->last_success_time.tv_sec;
      }
    }
  }
}

return_info AsyncRedisClusterAccess::async_connect(event_base* ae_event_loop, string ip,
                                                   long long timeout_millsec,
                                                   redis_cluster_connect_cb* conn_cb,
                                                   redis_cluster_disconnect_cb* disconn_cb,
                                                   void* user_data) {
  this->event_loop_ = ae_event_loop;
  this->m_ip = ip;
  this->timeout_millsec = timeout_millsec;
  this->m_redis_connect_cb = conn_cb;
  this->m_redis_disconnect_cb = disconn_cb;
  this->m_user_data = user_data;
  return async_connect();
}

void getCallback(redisClusterAsyncContext* c, void* r, void* privdata) {
  redisReply* reply = (redisReply*)r;
  if (reply == NULL) return;
  DEBUG_LOG("argv[%s]: %s\n", (char*)privdata, reply->str);

  /* Disconnect after receiving the reply to GET */
  // redisAsyncDisconnect(c);
}

return_info AsyncRedisClusterAccess::async_connect() {
  return_info ret;
  if (conn_state == CONN_STATE_CONNECTING || conn_state == CONN_STATE_ON) {
    return ret;
  }
  // std::lock_guard<std::mutex> lck(mx_);
  if (async_redis_ctx_) {
    async_disconnect();
  }
  //  async_redis_ctx_ = redisClusterAsyncConnect(m_ip.c_str(), HIRCLUSTER_FLAG_NULL);
  int timeout_ms = 200;
  struct timeval tv = {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  async_redis_ctx_ = redisClusterAsyncConnect(m_ip.c_str(), tv, HIRCLUSTER_FLAG_NULL);

  if (!async_redis_ctx_) {
    ERR_LOG("redis cluster async connect ctx is null");
    SET_ERROR_RETURN_INFO(ret, REDIS_CONNECT_ERROR, "redisClusterAsyncConnect:redisContext null.")
    return ret;
  }
  if (async_redis_ctx_->err != REDIS_OK) {
    ERR_LOG("redis cluster async connect ctx err:%d", async_redis_ctx_->err);
    SET_ERROR_RETURN_INFO(ret, REDIS_CONNECT_ERROR, "redisClusterAsyncConnect error:")
    ret.message += async_redis_ctx_->errstr;
    redisClusterAsyncFree(async_redis_ctx_);
    async_redis_ctx_ = NULL;
    return ret;
  }

  redisClusterLibeventAttach(async_redis_ctx_, event_loop_);

  async_redis_ctx_->data = (void*)this;

  if (redisClusterAsyncSetConnectCallback(async_redis_ctx_, redis_connect_callback) != REDIS_OK ||
      redisClusterAsyncSetDisconnectCallback(async_redis_ctx_, redis_disconnect_callback) !=
          REDIS_OK) {
    SET_ERROR_RETURN_INFO(ret, REDIS_CONNECT_ERROR, "set (dis)connect callback error.")
    return ret;
  }
  // redisClusterAsyncCommand(async_redis_ctx_, redis_ping_callback, (void*)"1", "ping");
  redisClusterAsyncCommand(async_redis_ctx_, getCallback, (char*)"eee", "GET key");
  conn_state = CONN_STATE_CONNECTING;
  return ret;
}

void AsyncRedisClusterAccess::async_disconnect() {
  if (!async_redis_ctx_ || conn_state == CONN_STATE_ON || CONN_STATE_CONNECTING == conn_state) {
    return;
  }
  // LOG(INFO) << "redis disconnection.[ip=" << m_ip.c_str() << ",port=" << m_port << "].";
  redisClusterAsyncDisconnect(async_redis_ctx_);
  conn_state = CONN_STATE_DISCONNECTING;
  async_redis_ctx_ = NULL;
  /*if(CONN_STATE_CONNECTING == conn_state || CONN_STATE_DISCONNECTING == conn_state){
          return;
  }
  if(async_redis_ctx_){
          LOG(INFO) << "redis disconnect free redis.[ip=" << m_ip.c_str() << ",port=" << m_port <<
  "]."; conn_state = CONN_STATE_DISCONNECTING; redisAsyncFree(async_redis_ctx_); async_redis_ctx_ =
  NULL;
  }*/
}
void AsyncRedisClusterAccess::async_reconnect() {
  async_disconnect();
  async_connect();
}

int AsyncRedisClusterAccess::get(const char* key, redis_cluster_cmd_cb* cb, void* privdata) {
  // std::lock_guard<std::mutex> lck(mx_);
  if (!async_redis_ctx_ || has_conn_err) {
    ERR_LOG("get conn err1");
    async_reconnect();
    return -1;
  }
  DEBUG_LOG("redisClster asycn command key:%s, ip:%s", key, m_ip.c_str());
  int ret = redisClusterAsyncCommand(async_redis_ctx_, cb, privdata, "GET %s", key);
  if (ret != REDIS_OK) {
    ERR_LOG("get conn err2");
    has_conn_err = true;
    return -1;
  }
  return 0;
}

int AsyncRedisClusterAccess::get_list_from_hashmap(const char* key, vector<string>& field_list,
                                                   redis_cluster_cmd_cb* cb, void* privdata) {
  int start_pos = 0;
  int end_pos = field_list.size();
  return get_list_from_hashmap(key, field_list, start_pos, end_pos, cb, privdata);
}

int AsyncRedisClusterAccess::get_list_from_hashmap(const char* key, vector<string>& field_list,
                                                   int& start_pos, int& end_pos,
                                                   redis_cluster_cmd_cb* cb, void* privdata) {
  if (!async_redis_ctx_ || has_conn_err) {
    return -1;
  }
  if (start_pos < 0) {
    start_pos = 0;
  }
  if ((size_t)end_pos > field_list.size()) {
    end_pos = field_list.size();
  }
  if (end_pos - start_pos <= 0) {
    return -1;
  }
  size_t val_count = end_pos - start_pos + 2;
  const char** vals = new const char*[(int)val_count];
  if (!vals) {
    // LOG(ERROR) << "new object failed.";
    return -1;
  }
  size_t* val_lens = new size_t[val_count];
  if (!val_lens) {
    // LOG(ERROR) << "new object failed.";
    delete[] vals;
    return -1;
  }
  // set command
  static char rpush_cmd[] = "HMGET";
  char cmd[256];
  int pret;
  int offset = 0;
  pret = snprintf(cmd, 256, "%s %s ", rpush_cmd, key);
  int size = field_list.size();
  for (int i = 0; i < size; i++) {
    offset += pret;
    if (offset > 256) {
      break;
    }
    pret = snprintf(cmd + offset, 256 - offset, "%s ", field_list[i].c_str());
  }

  int ret = redisClusterAsyncCommand(async_redis_ctx_, cb, privdata, cmd);
  if (ret != REDIS_OK) {
    has_conn_err = true;
    ret = -1;
  } else {
    ret = 0;
  }

  return ret;
}

int AsyncRedisClusterAccess::parse_reply(redisReply* reply, vector<string>& datas) {
  if (!reply || reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ERROR) {
    return -1;
  }
  redisReply* r = NULL;
  for (size_t i = 0; i < reply->elements; i++) {
    r = reply->element[i];
    datas.push_back((r && r->str) ? r->str : "");
  }
  return 0;
}

int AsyncRedisClusterAccess::parse_reply(redisReply* reply, string& data) {
  if (!reply || reply->type == REDIS_REPLY_ERROR) {
    return -1;
  }
  if (reply->str) {
    data = reply->str;
  } else {
    data = "";
  }
  return 0;
}
int AsyncRedisClusterAccess::parse_reply(redisReply* reply, int& data) {
  if (!reply || reply->type != REDIS_REPLY_INTEGER) {
    return -1;
  }
  data = reply->integer;
  return 0;
}
