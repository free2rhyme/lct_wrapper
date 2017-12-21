/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_conn_pool.cpp
 * @version     1.0
 * @date        Oct 9, 2017 11:53:22 AM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#include "lct_cass_conn_pool.h"
#include "lct_log.h"

#define NUM_THREADS                 1
#define NUM_IO_WORKER_THREADS       4
#define NUM_CONCURRENT_REQUESTS     10000
#define NUM_ITERATIONS              1000

CassCluster* LctCreateCluster() {
   CassCluster* cluster = cass_cluster_new();
   //cass_cluster_set_contact_points(cluster, hosts);
   //cass_cluster_set_credentials(cluster, "cassandra", "cassandra");
   cass_cluster_set_num_threads_io(cluster, 4);
   cass_cluster_set_queue_size_io(cluster, 50000);
   cass_cluster_set_pending_requests_low_water_mark(cluster, 30000);
   cass_cluster_set_pending_requests_high_water_mark(cluster, 50000);
   cass_cluster_set_core_connections_per_host(cluster, 2);
   cass_cluster_set_max_connections_per_host(cluster, 3);
   cass_cluster_set_max_requests_per_flush(cluster, 30000);
   return cluster;
}

LCT_ERR_CODE CLctCassConnPool::session(const LctCassKeyspaceType& keyspace, LctCassSessionType& cassSession)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   errCode = retrieveSession(keyspace, cassSession);
   if (LCT_SUCCESS != errCode) {
      errCode = connect(keyspace);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << "Failed to connect to keyspace(" << keyspace << ")";
         return errCode;
      }

      errCode = retrieveSession(keyspace, cassSession);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << "Failed to retrieve a cass session for keyspace(" << keyspace << ")";
         return errCode;
      }
   }

   return errCode;
}

LCT_ERR_CODE CLctCassConnPool::close()
{
   CCassConnLockGuard lg(m_rwlock);

   m_sessionMap.clear();
   m_clusterMap.clear();

   return LCT_SUCCESS;
}

LCT_ERR_CODE CLctCassConnPool::connect(const LctCassKeyspaceType& keyspace)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CassError csErr = CASS_OK;

   LctCassClusterType cluster(LctCreateCluster(), LctCassClusterRelease);
   LctCassSessionType session(cass_session_new(), LctCassSessionRelease);

   cass_cluster_set_contact_points(cluster.get(), m_dbHost.c_str());
   cass_cluster_set_port(cluster.get(), m_dbPort);

   LctCassFutureType connFuture(cass_session_connect(session.get(), cluster.get()));

   cass_future_wait(connFuture.get());
   csErr = cass_future_error_code(connFuture.get());
   if (CASS_OK != csErr) {
      LOG_ERROR << "Failed to connect to cassandra server(" << m_dbHost << ":" << m_dbPort << ")";
      errCode = LCT_FAIL;
      return errCode;
   }

   {
      CCassConnLockGuard lg(m_rwlock);
      errCode = appendSession(keyspace, session);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << "Failed to append a cass session for keyspace(" << keyspace << ")";
         return errCode;
      }

      errCode = appendCluster(keyspace, cluster);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << "Failed to append a cass cluster for keyspace(" << keyspace << ")";
         return errCode;
      }
   }

   return errCode;
}

CLctCassConnPool::CLctCassConnPool(const uint16_t dbPort, const std::string& dbHost):
   m_dbPort(dbPort),
   m_dbHost(dbHost)
{
}

CLctCassConnPool::~CLctCassConnPool()
{
}

