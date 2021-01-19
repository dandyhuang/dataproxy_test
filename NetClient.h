#ifndef NETCLIENT_H
#define NETCLIENT_H

#include "hircluster.h"
#include "ComType.h"
#include "wbl_comm.h"
#include "pngclient.h"
#include "tair_client_api.hpp"
#include "mutexlock.h"
#include "newmysql.h"
#include "DataProxySessionMsg.h"
#include "DynamicLoadConfig.h"
#include "redis_access.h"
#include "async_redis_access.h"
#include "async_redis_cluster_access.h"


class NetClient {
public:
	virtual ~NetClient(){};
	virtual bool SendMessage(CDataProxySessionMsg* session , CCommQuery* query)=0;
	virtual void stop(){}
	virtual void FiveMinuterTimer(){};
	std::string GetGroupName()
	{
		return m_GroupName;
	}
protected:
	string m_GroupName;
};


class NetClientMgn : public DynamicLoadConfig {
public:
	NetClientMgn() {}
	~NetClientMgn() {}
	bool init();
	void stop();

	int initConfig(char *err, CNewMysql *mysql);
	int RefreshConfig(char *err, CNewMysql *mysql);
	int LoadTairAsyncConfig(CSelectResult& result, CNewMysql *mysql, char *err);
	bool SetTairAsyncResult(CSelectResult& result, std::map<std::string, TdeAsyncModuleConfig> &freshTairAsyncConfig, char *err);
	int LoadRedisClusterSyncConfig(CSelectResult& result, CNewMysql *mysql, char *err);
	bool SetRedisClusterSyncResult(CSelectResult& result, std::map<std::string, SyncRedisClusterModuleConfig> &freshRedisClusterSyncConfig, char *err);
	int LoadPegasusAsyncConfig(CSelectResult& result, CNewMysql *mysql, char *err);
	bool SetPegasusAsyncConfig(CSelectResult& result, std::map<std::string, PegasusAsyncModuleConfig> &freshPegasusAsyncConfig, char *err);

	bool InitTairAsyncNetClient(std::map<std::string, TdeAsyncModuleConfig> &tairAsyncConfig, std::map<std::string, std::shared_ptr<NetClient>> &tairAsyncNetClient);
	bool InitRedisClusterAsyncNetClient(std::map<std::string, SyncRedisClusterModuleConfig> &redisClusterSyncConfig, std::map<std::string, std::shared_ptr<NetClient>> &redisClusterSyncNetClient);
	bool InitPegasusAsyncNetClient(std::map<std::string, PegasusAsyncModuleConfig> &pegasusAsyncConfig, std::map<std::string, std::shared_ptr<NetClient>> &pegasusAsyncNetCLient);
  bool InitAsyncRedisNetClient(std::map<std::string, SyncRedisClusterModuleConfig>& redis_config,
    std::map<std::string, std::shared_ptr<NetClient>>& ptr_net_client);
	int32_t SendMessage(CDataProxySessionMsg* session, vector<ExtendSoQuery>& queryVec);
	
	void FiveMinuterTimer();

	std::shared_ptr<NetClient> GetNetClient(const std::string &groupname)
	{
		std::lock_guard<std::mutex> guard(m_mutex);
		auto client_iter = m_onlineNetClient.find(groupname);
		if (client_iter != m_onlineNetClient.end()) {
			return client_iter->second;
		}

		return nullptr;
	}
private:
	static const char *const s_tairLogLevelStr[]; 

	std::mutex m_mutex;
	std::string m_tairAsyncConfigLastUpdateTime;
	std::string m_redisClusterSyncConfigLastUpdateTime;
	std::string m_pegasusAsyncConfigLastUpdateTime;
	std::map<std::string, TdeAsyncModuleConfig> m_tairAsyncConfig;
	std::map<std::string, SyncRedisClusterModuleConfig> m_redisClusterSyncConfig;
	std::map<std::string, SyncRedisClusterModuleConfig> async_redis_config_;
	std::map<std::string, PegasusAsyncModuleConfig> m_pegasusAsyncConfig;
	
	std::map<std::string, std::shared_ptr<NetClient>> m_onlineNetClient;
	std::map<time_t, std::vector<std::shared_ptr<NetClient>>> m_deferDeleteNetClient;

	bool async_flag_ = false;
};

#endif
