/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_conn_pool.inl
 * @version     1.0
 * @date        Nov 1, 2017 6:49:44 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_CONN_POOL_INL_
#define SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_CONN_POOL_INL_


inline LCT_ERR_CODE CLctCassConnPool::retrieveSession(const LctCassKeyspaceType& keyspace, LctCassSessionType& cassSession)
{
   CCassConnLockGuard lg(m_rwlock);

   auto itRe = m_sessionMap.find(keyspace);
   if (m_sessionMap.cend() == itRe) {
      return LCT_NOT_SUCH_RECORD;
   }
   cassSession = itRe->second;

   return LCT_SUCCESS;
}

inline LCT_ERR_CODE CLctCassConnPool::release(const LctCassKeyspaceType& keyspace)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CCassConnLockGuard lg(m_rwlock);
   {
      auto it = m_sessionMap.find(keyspace);
      if (m_sessionMap.end() != it) {
         m_sessionMap.erase(it);
      }
   }

   {
      auto it = m_clusterMap.find(keyspace);
      if (m_clusterMap.end() != it) {
         m_clusterMap.erase(it);
      }
   }

   return errCode;
}

inline LCT_ERR_CODE CLctCassConnPool::appendSession(const LctCassKeyspaceType& keyspace, const LctCassSessionType& cassSession)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   m_sessionMap[keyspace] = cassSession;

   return errCode;
}

inline LCT_ERR_CODE CLctCassConnPool::appendCluster(const LctCassKeyspaceType& keyspace, const LctCassClusterType& cluster)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   m_clusterMap[keyspace] = cluster;

   return errCode;
}

#endif /* SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_CONN_POOL_INL_ */
