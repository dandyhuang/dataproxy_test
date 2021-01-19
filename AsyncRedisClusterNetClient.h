#ifndef _ASYNC_REDIS_CLUSTER_NET_CLIENT_H_
#define _ASYNC_REDIS_CLUSTER_NET_CLIENT_H_

#include <memory>
#include <mutex>
#include <vector>

#include "NetClient.h"
#include "async_redis_cluster_access.h"
#include "mutexlock.h"
#include "pngclient.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "event2/thread.h"
#ifdef __cplusplus
}
#endif
class AsyncRedisClusterNetClient;
struct RedisAsyncContext {
 public:
  RedisAsyncContext(NetClient* cli, RedisRspInfo* rsp_info) {
    client = cli;
    rsp = *rsp_info;
  }
  NetClient* get_client() { return client; }
  RedisRspInfo get_redis_rsp() { return rsp; }
  NetClient* client = nullptr;
  RedisRspInfo rsp;
};

class AsyncRedisClusterHandleThread : public PNG::CPngThread {
 public:
  using ptr = std::shared_ptr<AsyncRedisClusterHandleThread>;

 public:
  AsyncRedisClusterHandleThread(event_base* event_loop) : event_loop_(event_loop) {}
  ~AsyncRedisClusterHandleThread() {}

  int run() override;

 private:
  event_base* event_loop_ = nullptr;
};

class AsyncRedisClusterProcessThread : public PNG::CPngThread {
 public:
  using ptr = std::shared_ptr<AsyncRedisClusterProcessThread>;

 public:
  AsyncRedisClusterProcessThread(AsyncRedisClusterAccess::ptr access,
                                 AsyncRedisClusterNetClient* client, event_base* event_loop) {
    // std::lock_guard<std::mutex> lck(mx_);

    redis_access_ = access;
    net_client_ = client;
    event_loop_ = event_loop;
  }
  ~AsyncRedisClusterProcessThread() {}

  int run();
  void ExcuteGetCmd(CCommQuery* query, CCommReply* reply);
  void ExcuteSetCmd(CCommQuery* query, CCommReply* reply);
  void ExcuteHmGetCmd(CCommQuery* query, CCommReply* reply);

 private:
  AsyncRedisClusterAccess::ptr redis_access_;
  AsyncRedisClusterNetClient* net_client_ = nullptr;
  event_base* event_loop_ = nullptr;
  // std::mutex mx_;
};

class AsyncRedisClusterNetClient : public std::enable_shared_from_this<AsyncRedisClusterNetClient>,
                                   public NetClient {
 public:
  using ptr = std::shared_ptr<AsyncRedisClusterNetClient>;

 public:
  AsyncRedisClusterNetClient() {
    if (-1 == evthread_use_pthreads()) {
      ERR_LOG("use thread-safe event failed");
    }
  }
  ~AsyncRedisClusterNetClient();
  bool Init(SyncRedisClusterModuleConfig& config);
  bool SendMessage(CDataProxySessionMsg* session, CCommQuery* query);
  bool ExecCommand(AsyncRedisClusterAccess::ptr ars, CCommQuery* query);
  CCommQuery* GetMessage();
  void stop() override;
  void Handle(RedisRspInfo* rspinfo, const char* buf, unsigned int len);

 private:
  CTMsgQueueCond<CCommQuery, CTMutex> queue_;
  std::vector<AsyncRedisClusterHandleThread::ptr> handle_thread_pool_;
  std::vector<AsyncRedisClusterProcessThread::ptr> queue_thread_pool_;
  event_base* event_loop_ = nullptr;
};

#endif
