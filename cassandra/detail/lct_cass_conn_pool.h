/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cassandra_connection.h
 * @version     1.0
 * @date        Sep 26, 2017 11:28:19 AM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#ifndef SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_POOL_H_
#define SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_POOL_H_

#include "lct_common_define.h"
#include "lct_cass_common_define.h"
#include "lct_rw_lock.h"
#include <condition_variable>

class CLctCassConnPool final
{
public:
   LCT_ERR_CODE session(const LctCassKeyspaceType& keyspace, LctCassSessionType& cassSession);
   LCT_ERR_CODE release(const LctCassKeyspaceType& keyspace);
   LCT_ERR_CODE close();

private:
   LCT_ERR_CODE connect(const LctCassKeyspaceType& keyspace);
   LCT_ERR_CODE appendSession(const LctCassKeyspaceType& keyspace, const LctCassSessionType& cassSession);
   LCT_ERR_CODE appendCluster(const LctCassKeyspaceType& keyspace, const LctCassClusterType& cluster);
   LCT_ERR_CODE retrieveSession(const LctCassKeyspaceType& keyspace, LctCassSessionType& cassSession);

public:
   CLctCassConnPool(const uint16_t dbPort, const std::string& dbHost);
   ~CLctCassConnPool();

   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctCassConnPool);

private:
   const uint16_t           m_dbPort;
   const std::string        m_dbHost;

   typedef std::unordered_map<LctCassKeyspaceType, LctCassSessionType> LctCassSessionMapType;
   typedef std::unordered_map<LctCassKeyspaceType, LctCassClusterType> LctCassClusterMapType;
   typedef CLctRwlockGuard   CCassConnLockGuard;

   mutable CLctRwLock       m_rwlock;
   LctCassSessionMapType    m_sessionMap;
   LctCassClusterMapType    m_clusterMap;
};

#include "lct_cass_conn_pool.inl"

#endif /* SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_POOL_H_ */
