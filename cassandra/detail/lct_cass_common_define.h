/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_common_define.h
 * @version     1.0
 * @date        Oct 12, 2017 5:38:39 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_COMMON_DEFINE_H_
#define SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_COMMON_DEFINE_H_

#include <string>
#include <memory>
#include <list>
#include "cassandra.h"

inline void LctCassSessionRelease(CassSession* session)
{
   if (nullptr != session) {
      cass_session_close(session);
      cass_session_free(session);
   }
}

inline void LctCassClusterRelease(CassCluster* cluster)
{
   if (nullptr != cluster) {
      cass_cluster_free(cluster);
   }
}

inline void LctCassFutureRelease(CassFuture* future)
{
   if (nullptr != future) {
      cass_future_free(future);
   }
};

inline void LctCassStatementRelease(CassStatement* statement)
{
   if (nullptr != statement) {
      cass_statement_free(statement);
   }
};

inline void LctCassResultRelease(const CassResult* result)
{
   if (nullptr != result) {
      cass_result_free(result);
   }
};

inline void LctCassBatchRelease(CassBatch* batch)
{
   if (nullptr != batch) {
      cass_batch_free(batch);
   }
};

struct CLctCassFutureRelease
{
   void operator()(CassFuture* future) const
   {
      if (nullptr != future) {
         cass_future_free(future);
      }
   }
};

struct CLctCassStatementRelease
{
   void operator()(CassStatement* statement) const
   {
      if (nullptr != statement) {
         cass_statement_free(statement);
      }
   }
};

struct CLctCassResultRelease
{
   void operator()(const CassResult* result) const
   {
      if (nullptr != result) {
         cass_result_free(result);
      }
   }
};

struct CLctCassIteratorRelease
{
   void operator()(CassIterator* rows) const
   {
      if (nullptr != rows) {
         cass_iterator_free(rows);
      }
   }
};

struct CLctCassBatchRelease
{
   void operator()(CassBatch* batch) const
   {
      if (nullptr != batch) {
         cass_batch_free(batch);
      }
   }
};

typedef std::string                                            LctCassKeyspaceType;
typedef std::shared_ptr<CassSession>                           LctCassSessionType;
typedef std::shared_ptr<CassCluster>                           LctCassClusterType;
typedef std::shared_ptr<CassFuture>                            LctCassFutureShpType;
typedef std::shared_ptr<CassStatement>                         LctCassStatementShpType;
typedef std::shared_ptr<const CassResult>                      LctCassResultShpType;
typedef std::shared_ptr<CassBatch>                             LctCassBatchType;

typedef std::unique_ptr<CassFuture,        CLctCassFutureRelease>      LctCassFutureType;
typedef std::unique_ptr<CassStatement,     CLctCassStatementRelease>   LctCassStatementType;
typedef std::unique_ptr<const CassResult,  CLctCassResultRelease>      LctCassResultType;
typedef std::unique_ptr<CassIterator,      CLctCassIteratorRelease>    LctCassIteratorType;

typedef std::list<LctCassFutureShpType>     LctCassFutureShpContainerType;

struct LctCassPageInfo
{
   LctCassPageInfo()
   {
   }

   LctCassPageInfo(const LctCassStatementShpType& cs, const LctCassResultShpType& cr):
      CassStatement(cs),
      CassResult(cr)
   {
   }
   LctCassStatementShpType  CassStatement;
   LctCassResultShpType     CassResult;
   int64_t                  Timestamp      = 0;
};

static constexpr std::size_t CASSANDRA_MAX_LIMIT_DEFAULT_SIZE   = 20;
static constexpr std::size_t CASSANDRA_RESTRAIN_WAITING_SECONDS = 30;

#endif /* SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_COMMON_DEFINE_H_ */
