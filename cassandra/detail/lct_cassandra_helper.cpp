/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cassandra_helper.cpp
 * @version     1.0
 * @date        Sep 26, 2017 11:22:12 AM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#include "lct_cassandra_helper.h"
#include "lct_cass_access_restrain.h"
#include "lct_cass_conn_pool.h"
#include "lct_cplusplus_14.h"
#include "lct_log.h"
#include <algorithm>

LCT_ERR_CODE CLctCassandraHelper::init(const uint16_t dbPort, const std::string& dbHost)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   m_connPool = std::make_unique<CLctCassConnPool>(dbPort, dbHost);

   m_ready.store(true);

   const std::size_t hostCount = std::count(dbHost.begin(), dbHost.end(), ';');
   if (hostCount > 0) {
      setRequestMax(CASSANDRA_MAX_LIMIT_DEFAULT_SIZE * hostCount);
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::createTable(const std::string& keyspace, const std::string& cqlStatement)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   errCode = execute(keyspace, cqlStatement);
   if (LCT_SUCCESS != errCode) {
      if (LCT_RECORD_EXISTS == errCode) {
         LOG_ERROR << "Table exists already";
         errCode = LCT_SUCCESS;
      } else {
         LOG_ERROR << "Failed to create table statement(" << cqlStatement << ")";
      }
      return errCode;
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::insert(const std::string& keyspace, const std::string& cqlStatement)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   errCode = execute(keyspace, cqlStatement);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to insert record for statement(" << cqlStatement << ")";
      return errCode;
   }

   return errCode;
}


LCT_ERR_CODE CLctCassandraHelper::update(const std::string& keyspace, const std::string& cqlStatement)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   errCode = execute(keyspace, cqlStatement);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to update record for statement(" << cqlStatement << ")";
      return errCode;
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::remove(const std::string& keyspace, const std::string& cqlStatement)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   errCode = execute(keyspace, cqlStatement);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to remove record for statement(" << cqlStatement << ")";
      return errCode;
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::batchExec(const std::string& keyspace, const std::vector<std::string>& cqlStatements)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   const std::size_t cqlCount = cqlStatements.size();

   if (0 == cqlCount) {
      return errCode;
   }

   std::size_t byteSize = 0;

   LctCassBatchType batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED), LctCassBatchRelease);

   auto itRe = cqlStatements.cbegin();
   while (itRe != cqlStatements.cend()) {
      const auto& cql = *itRe;

      if (byteSize + cql.size() < m_batchExecChunkSize) {
         LctCassStatementType statement(cass_statement_new(cql.c_str(), 0));
         CassError csErr = cass_batch_add_statement(batch.get(), statement.get());
         if (CASS_OK != csErr) {
            LOG_ERROR << "Failed to add statement(" << cql << ") into batch error(" << cassErrDesc(csErr) << ")";
            errCode = LCT_FAIL;
            return errCode;
         }
         byteSize += cql.size();
         if ((itRe + 1) != cqlStatements.cend()) {
            ++itRe;
            continue;
         } else {
            ++itRe;
         }
      }

      if (0 == byteSize) {
         errCode  = insert(keyspace, cql);
         if (LCT_SUCCESS != errCode) {
            LOG_ERROR << "Failed to insert statement " << ErrCodeFormat(errCode);
            break;
         }
         ++itRe;
      } else {
         errCode = batchExec(keyspace, batch);
         if (LCT_SUCCESS != errCode) {
            LOG_ERROR << "Failed to execute batch statement " << ErrCodeFormat(errCode);
            break;
         }
         byteSize = 0;
         batch.reset(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED), LctCassBatchRelease);
      }
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::createKeyspace(const std::string& keyspace, const int32_t replicFactor /* = 3 */)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   std::stringstream ss;
   ss << " CREATE KEYSPACE IF NOT EXISTS " << keyspace;
   ss << " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" << replicFactor;
   ss << " };";

   errCode = execute(keyspace, ss.str());
   if (LCT_SUCCESS != errCode) {
      if (LCT_RECORD_EXISTS == errCode) {
         LOG_ERROR << "Keyspace(" << keyspace << ") exists already";
         errCode = LCT_SUCCESS;
      } else {
         LOG_ERROR << "Failed to create Keyspace statement(" << ss.str() << ")";
      }
      return errCode;
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::close()
{
   if (!m_ready.exchange(false)) {
      return LCT_SUCCESS;
   }
   return m_connPool->close();
}

LCT_ERR_CODE CLctCassandraHelper::execute(
      const std::string& keyspace,
      const std::string& cqlStatement,
      const CassResult** results /* = nullptr */)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   if (!m_ready.load()) {
      LOG_ERROR << "CassandraHelper is not ready yet";
      return LCT_NOT_INITIATED_YET;
   }

   CassAutoFlowControl();

   LctCassSessionType cassSession;
   errCode = m_connPool->session(keyspace, cassSession);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to get a session for keyspace(" << keyspace << ")";
      return errCode;
   }

   LOG_DEBUG << "CassandraHelper is executing:" << cqlStatement;

   LctCassStatementType statement(cass_statement_new(cqlStatement.c_str(), 0));

   LctCassFutureType resultFuture(cass_session_execute(cassSession.get(), statement.get()));

   cass_future_wait(resultFuture.get());

   CassError csErr = cass_future_error_code(resultFuture.get());
   if (CASS_OK != csErr) {
      LOG_ERROR << "Failed to execute statement(" << cqlStatement << ") error(" << cassErrDesc(csErr) << ")";
      if (CASS_ERROR_SERVER_ALREADY_EXISTS == csErr) {
         errCode = LCT_RECORD_EXISTS;
      } else {
         errCode = LCT_FAIL;
      }
      return errCode;
   }

   if (nullptr != results) {
      *results = cass_future_get_result(resultFuture.get());
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::execute(
      const std::string& keyspace,
      const std::vector<std::string>& cqlStatements,
      LctCassFutureShpContainerType& futureShpContainer)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   if (!m_ready.load()) {
      LOG_ERROR << "CassandraHelper is not ready yet";
      return LCT_NOT_INITIATED_YET;
   }

   CassAutoFlowControl();

   LctCassSessionType cassSession;
   errCode = m_connPool->session(keyspace, cassSession);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to get a session for keyspace(" << keyspace << ")";
      return errCode;
   }

   for (const auto& cqlStatement: cqlStatements) {

      LOG_DEBUG << cqlStatement;

      LctCassStatementType statement(cass_statement_new(cqlStatement.c_str(), 0));
      futureShpContainer.emplace_back(cass_session_execute(cassSession.get(), statement.get()), LctCassFutureRelease);
   }

   return errCode;
}


LCT_ERR_CODE CLctCassandraHelper::batchExec(const std::string& keyspace, const LctCassBatchType& cqlStatements)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CassAutoFlowControl();

   LctCassSessionType cassSession;
   errCode = m_connPool->session(keyspace, cassSession);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << "Failed to get a session for keyspace(" << keyspace << ")";
      return errCode;
   }

   LctCassFutureType resultFuture(cass_session_execute_batch(cassSession.get(), cqlStatements.get()));

   cass_future_wait(resultFuture.get());

   CassError csErr = cass_future_error_code(resultFuture.get());
   if (CASS_OK != csErr) {
      LOG_ERROR << "Failed to execute batch statement error(" << cassErrDesc(csErr) << ")";
      if (CASS_ERROR_SERVER_INVALID_QUERY == csErr) {
         errCode = LCT_BATCH_SIZE_OVERFLOW;
      } else if (CASS_ERROR_LIB_NO_HOSTS_AVAILABLE == csErr) {
         errCode = LCT_CASS_NO_HOST;
      } else {
         errCode = LCT_FAIL;
      }
      return errCode;
   }

   return errCode;
}

LCT_ERR_CODE CLctCassandraHelper::setRequestMax(const std::size_t limit)
{
   if (!m_ready.load()) {
      return LCT_FAIL;
   }
   LOG_INFOR << "Cassandra helper is setting request max(" << limit << ")";

   CCassRestrain::setConcurrentMax(limit);

   return LCT_SUCCESS;
}

std::size_t CLctCassandraHelper::requestMax() const
{
   if (!m_ready.load()) {
      return 0;
   }
   return CCassRestrain::limit();
}

