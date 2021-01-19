#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <thread>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif
#include "adapters/libevent.h"
#include "async.h"
#include "event2/thread.h"
#include "hircluster.h"
#include "hiredis.h"
#ifdef __cplusplus
}
#endif

/* Put event loop in the global scope, so it can be explicitly stopped */
void getCallback(redisClusterAsyncContext* c, void* r, void* privdata) {
  redisReply* reply = (redisReply*)r;
  if (reply == NULL) return;
  printf("argv[%s]: %s\n", (char*)privdata, reply->str);

  /* Disconnect after receiving the reply to GET */
  // redisAsyncDisconnect(c);
}

void connectCallback(const redisAsyncContext* c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    // aeStop(loop);
    return;
  }

  printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext* c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    // aeStop(loop);
    return;
  }

  printf("Disconnected...\n");
  // aeStop(loop);
}
class AsyncEventThread {
 public:
  AsyncEventThread(char* address) {
    address_ = (const char*)address;
    evthread_use_pthreads();
    for (int i = 0; i < 5; ++i) {
      struct event_base* base = event_base_new();
      if (!base) {
        printf("event_base_new failed\n");
        return;
      }
      event_queue_.push_back(base);
      worker_threads_.push_back(std::unique_ptr<std::thread>(
          new std::thread(&AsyncEventThread::workThread, this, base, i)));
    }

    for (int i = 0; i < 20; i++) {
      int index = i % 5;
      auto base = event_queue_[index];
      int timeout_ms = 200;
      struct timeval tv = {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
      ctx_ = redisClusterAsyncConnect(address_.c_str(), tv, HIRCLUSTER_FLAG_NULL);
      ctxs_.push_back(ctx_);
      if (ctx_->err) {
        printf("error: %s\n", ctx_->errstr);
        return;
      }
      if (ctx_->err != REDIS_OK) {
        redisClusterAsyncFree(ctx_);
        ctx_ = NULL;
        return;
      }
      redisClusterLibeventAttach(ctx_, base);
      redisClusterAsyncSetConnectCallback(ctx_, connectCallback);
      redisClusterAsyncSetDisconnectCallback(ctx_, disconnectCallback);
      // redisClusterAsyncCommand(ctx_, getCallback, (char*)"eee", "GET dandy");
      handle_threads_.push_back(std::unique_ptr<std::thread>(
          new std::thread(&AsyncEventThread::handleThread, this, ctx_)));
    }
  }

  ~AsyncEventThread() {}
  void handleThread(redisClusterAsyncContext* ctx) {
    sleep(1);
    while (thread_status_) {
      redisClusterAsyncCommand(ctx, getCallback, (char*)"1", "GET dandy");
      redisClusterAsyncCommand(ctx, getCallback, (char*)"1", "GET dandy");
      redisClusterAsyncCommand(ctx, getCallback, (char*)"2", "GET dandy");
      redisClusterAsyncCommand(ctx, getCallback, (char*)"3", "GET dandy");
      redisClusterAsyncCommand(ctx, getCallback, (char*)"4", "GET dandy");
      redisClusterAsyncCommand(ctx, getCallback, (char*)"5", "GET dandy");
      // redisClusterAsyncCommand(ctx, getCallback, (char*)"6", "GET dandy");
      // redisClusterAsyncCommand(ctx, getCallback, (char*)"7", "GET dandy");
      // redisClusterAsyncCommand(ctx, getCallback, (char*)"8", "GET dandy");
      // redisClusterAsyncCommand(ctx, getCallback, (char*)"9", "GET dandy");
      // CMessageCollector::GetInstance()->SetResponseMessage(reply);

      printf("thread_fun get\n");
      usleep(100);
    }
  }

  void workThread(struct event_base* base, uint32_t id) {
    // ::prctl(PR_SET_NAME, "libevent_thread");
    while (thread_status_) {
      int ret = event_base_loop(base, EVLOOP_NO_EXIT_ON_EMPTY);
      if (ret == 1)
        continue;
      else if (ret == 0)
        break;
      else
        printf("workThread event base %p event_base_loop return %d\n", base, ret);
    }
    event_base_free(base);
    printf("worker thread end~~\n");
  }
  void start();
  void Stop() {
    printf("stop==\n");
    for (auto& thread : worker_threads_) {
      if (thread->joinable()) thread->join();
    }
    printf("worker stop==\n");
    worker_threads_.clear();
    for (auto& thread : handle_threads_) {
      if (thread->joinable()) thread->join();
    }
    for (auto& it : event_queue_) event_base_loopbreak(it);

    handle_threads_.clear();
  }

  // event 队列
  std::vector<struct event_base*> event_queue_;
  // worker
  std::vector<std::unique_ptr<std::thread> > worker_threads_;
  // worker
  std::vector<std::unique_ptr<std::thread> > handle_threads_;
  bool thread_status_ = true;
  std::string address_;
  std::vector<redisClusterAsyncContext*> ctxs_;
  redisClusterAsyncContext* ctx_;
};

int main(int argc, char** argv) {
  signal(SIGPIPE, SIG_IGN);

  AsyncEventThread threads(argv[1]);
  sleep(1);
  threads.Stop();
  std::vector<std::thread> queue_threads;
  // for (int i = 0; i < 10; ++i) {
  //   queue_threads.push_back(std::thread(queue_thread_fun, argv[1]));
  // }

  // for (auto& thread : handle_threads) {
  //   thread.join();
  // }
  // for (auto& thread : queue_threads) {
  //   thread.join();
  // }
  return 0;
}
