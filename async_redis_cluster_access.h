#ifndef _ASYNC_REDIS_CLUSTER_ACCESS_H_
#define _ASYNC_REDIS_CLUSTER_ACCESS_H_

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include "CLog.h"
#include "redis_access.h"

using std::string;
using std::vector;

#ifdef __cplusplus
extern "C" {
#endif
#include "adapters/libevent.h"
#include "async.h"
#include "hircluster.h"
#include "hiredis.h"
#ifdef __cplusplus
}
#endif

#define CONNECT_REDIS_TIMEOUT_MILLSECONDS 500
#define REDIS_EXPIRE_KEY_SECONDS 300
#define REDIS_CONNECTION_TRY_MIN_MILLSECONDS 500
#define REDIS_DOG_TAG 0x1357ABCD

#define CONN_STATE_DOWN 0
#define CONN_STATE_CONNECTING 1
#define CONN_STATE_DISCONNECTING 2
#define CONN_STATE_ON 3

struct return_info;
class AsyncRedisClusterAccess;

typedef void(redis_cluster_connect_cb)(AsyncRedisClusterAccess*, void* /*user_data*/,
                                       bool /*success*/);
typedef void(redis_cluster_disconnect_cb)(AsyncRedisClusterAccess*, void* /*user_data*/);
typedef void(redis_cluster_cmd_cb)(redisClusterAsyncContext*, void* /*reply*/, void* /*privdata*/);

class AsyncRedisClusterAccess {
 public:
  AsyncRedisClusterAccess()
      : m_redis_connect_cb(NULL),
        m_redis_disconnect_cb(NULL),
        m_user_data(NULL),
        event_loop_(NULL),
        async_redis_ctx_(NULL),
        redis_dog_tag(REDIS_DOG_TAG),
        diff_local_time_seconds(0),
        timeout_millsec(CONNECT_REDIS_TIMEOUT_MILLSECONDS),
        conn_state(CONN_STATE_DOWN),
        has_conn_err(false) {}

  ~AsyncRedisClusterAccess() {
    if (async_redis_ctx_) {
      async_disconnect();
    }
    redis_dog_tag = 0;
  }
  using ptr = std::shared_ptr<AsyncRedisClusterAccess>;

 public:
  static void redis_disconnect_callback(const struct redisAsyncContext*, int status);
  static void redis_connect_callback(const struct redisAsyncContext*, int status);
  static void redis_ping_callback(redisClusterAsyncContext* ctx, void* r, void* privdata);
  static void redis_time_callback(redisClusterAsyncContext* ctx, void* r, void* privdata);
  static void redis_get_callback(redisClusterAsyncContext* ctx, void* r, void* privdata);
  static int parse_reply(redisReply* reply, vector<string>& datas);
  static int parse_reply(redisReply* reply, string& data);
  static int parse_reply(redisReply* reply, int& data);

 public:
  return_info async_connect(event_base* ae_event_loop, string ip,
                            long long timeout_millsec = CONNECT_REDIS_TIMEOUT_MILLSECONDS,
                            redis_cluster_connect_cb* conn_cb = NULL,
                            redis_cluster_disconnect_cb* disconn_cb = NULL, void* user_data = NULL);
  return_info async_connect();
  void async_disconnect();
  void async_reconnect();
  int exe_command(redis_cluster_cmd_cb* cb, void* privdata, const char* format, ...);
  int exe_command_argv(redis_cluster_cmd_cb* cb, void* privdata, int argc, const char** argv,
                       const size_t* argvlen);

  // int expire_data_seconds(const char* key, long seconds);
  // int set(const char* key, const char* value, redis_cmd_cb* cb, void* privdata);
  int get(const char* key, redis_cluster_cmd_cb* cb, void* privdata);

  int get_list_from_hashmap(const char* key, vector<string>& field_list, redis_cluster_cmd_cb* cb,
                            void* privdata);
  int get_list_from_hashmap(const char* key, vector<string>& field_list, int& start_pos,
                            int& end_pos, redis_cluster_cmd_cb* cb, void* privdata);

  bool can_use() {
    return (conn_state == CONN_STATE_ON) && async_redis_ctx_ != NULL && !has_conn_err &&
           redis_dog_tag == REDIS_DOG_TAG;
  }
  void set_error() { has_conn_err = true; }
  inline bool ping_redis();

 public:
  std::string m_ip;
  redis_cluster_connect_cb* m_redis_connect_cb = nullptr;
  redis_cluster_disconnect_cb* m_redis_disconnect_cb = nullptr;
  void* m_user_data = nullptr;
  event_base* event_loop_ = nullptr;
  redisClusterAsyncContext* async_redis_ctx_ = nullptr;
  timeval last_success_time;
  int redis_dog_tag = 0;
  int diff_local_time_seconds = 0;
  std::mutex mx_;
 private:
  int timeout_millsec = 300;  // connect timeout
  int conn_state = 0;
  bool has_conn_err = false;
};
inline bool AsyncRedisClusterAccess::ping_redis() {
  if (!async_redis_ctx_) {
    has_conn_err = true;
    return false;
  }
  if (redis_dog_tag != REDIS_DOG_TAG) {
    // LOG(WARNING) << "ping error: redis_dog_tag error.";
    return false;
  }
  return (redisClusterAsyncCommand(async_redis_ctx_, redis_ping_callback, this, "ping") == 0);
}

#endif  //_ASYNCREDISCLUSTERACCESS_H_
