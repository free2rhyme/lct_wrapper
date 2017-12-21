/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cassandra_helper.h
 * @version     1.0
 * @date        Sep 26, 2017 11:22:08 AM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#ifndef SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_HELPER_H_
#define SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_HELPER_H_

#include "detail/lct_cass_page_state_vessel.h"
#include "detail/lct_cass_common_define.h"
#include "lct_common_define.h"
#include "lct_singleton.h"
#include "cassandra.h"
#include <atomic>

enum class ELctCassReplicStrategy: uint8_t
{
	CASS_REPLIC_SIMPLE_STRATEGY    = 0,
	CASS_REPLIC_NETWORK_STRATEGY   = 1,
};

class CLctCassConnPool;
class CLctCassandraHelper final: public CLctSingleton<CLctCassandraHelper>
{
   static constexpr std::size_t CASSANDRA_BATCH_CHUNK_DEFAULT_SIZE     = 50 * 1024;       //50 kb
   static constexpr std::size_t CASSANDRA_PAGE_QUERY_DEFAULT_SIZE      =  4 * 1024;

public:
   LCT_ERR_CODE createTable(const std::string& keyspace, const std::string& cqlStatement);

   LCT_ERR_CODE createKeyspace(const std::string& keyspace, const int32_t replicFactor = 3);

   LCT_ERR_CODE insert(const std::string& keyspace, const std::string& cqlStatement);

   LCT_ERR_CODE update(const std::string& keyspace, const std::string& cqlStatement);

   template <typename Func>
   LCT_ERR_CODE query(const std::string& keyspace, const std::string& cqlStatement, Func fn, std::size_t* recordFound = nullptr);

   template <typename Func>
   LCT_ERR_CODE pageQuery(
         const std::string& keyspace,
         const std::string& cqlStatement,
         const bool isFirst,
         const int32_t pageSize,
         Func fn,
         bool& hasMoreRecord,
         std::size_t* recordFound);

   template <typename Func>
   LCT_ERR_CODE multiQuery(const std::string& keyspace, const std::vector<std::string>& cqlStatements, Func fn);

   LCT_ERR_CODE remove(const std::string& keyspace, const std::string& cqlStatement);

   LCT_ERR_CODE batchExec(const std::string& keyspace, const std::vector<std::string>& cqlStatements);

public:
   LCT_ERR_CODE init(const uint16_t dbPort, const std::string& dbHost);

   LCT_ERR_CODE close();

   LCT_ERR_CODE setChunkSize(const std::size_t size);

   std::size_t chunkSize() const;

   LCT_ERR_CODE setPageQuerySize(const std::size_t size);

   std::size_t pageQuerySize() const;

   LCT_ERR_CODE setRequestMax(const std::size_t limit);

   std::size_t requestMax() const;

private:
	LCT_ERR_CODE execute(
	      const std::string& keyspace,
	      const std::string& cqlStatement,
	      const CassResult** results = nullptr);

   LCT_ERR_CODE execute(
         const std::string& keyspace,
         const std::vector<std::string>& cqlStatements,
         LctCassFutureShpContainerType& futureShpContainer);

	const char* cassErrDesc(const CassError) const;

	LCT_ERR_CODE batchExec(
	      const std::string& keyspace,
	      const LctCassBatchType& cqlStatements);

   template <typename Func>
   LCT_ERR_CODE queryFirstPage(
         const std::string& keyspace,
         const std::string& cqlStatement,
         const int32_t pageSize,
         Func fn,
         bool& hasMoreRecord,
         std::size_t* recordFound);

   template <typename Func>
   LCT_ERR_CODE queryRestPage(
         const std::string& keyspace,
         const std::string& cqlStatement,
         const int32_t pageSize,
         Func fn,
         bool& hasMoreRecord,
         std::size_t* recordFound);

private:
	std::atomic_bool         m_ready{false};
   std::size_t              m_batchExecChunkSize = CASSANDRA_BATCH_CHUNK_DEFAULT_SIZE;
   std::size_t              m_pageQuerySize      = CASSANDRA_PAGE_QUERY_DEFAULT_SIZE;

	CLctCassPageStateVessel  m_pageStateVessel;

   typedef std::unique_ptr<CLctCassConnPool> CLctCassConnPoolType;
   CLctCassConnPoolType     m_connPool;
};

#define LCT_CASSANDRA_HELPER CLctCassandraHelper::instance()

#include "detail/lct_cassandra_helper.inl"
#include "detail/lct_cassandra_helper.hpp"

#endif /* SRC_WRAPPER_CASSANDRA_LCT_CASSANDRA_HELPER_H_ */


