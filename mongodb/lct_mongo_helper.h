/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_mongo_client_helper.h
 * @version     1.0
 * @date        Jun 23, 2017 3:51:38 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_H_
#define SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_H_

#include "lct_common_define.h"
#include "lct_singleton.h"
#include "lct_rw_lock.h"
#include <map>
#include <unordered_map>
#include <memory>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/exception/exception.hpp>

typedef std::vector<std::string>             CMongoFilter;
typedef bsoncxx::builder::stream::document   CMongoDocument;
typedef bsoncxx::builder::stream::array      CMongoArray;
typedef mongocxx::options::index             CMongoIndex;
typedef mongocxx::collection                 CMongoCollection;
typedef mongocxx::cursor                     CMongoCursor;
typedef mongocxx::client                     CMongoClient;
typedef mongocxx::options::find              CMongoFindOption;
typedef mongocxx::options::update            CMongoUpdateOption;
typedef mongocxx::options::count             CMongoCountOption;
typedef mongocxx::model::insert_one          CMongoInsertOne;
typedef mongocxx::bulk_write                 CMongoBulkWrite;
typedef mongocxx::exception                  CMongoException;

typedef mongocxx::pool                       CMongoPool;
typedef mongocxx::instance                   CMongoInstance;
typedef mongocxx::pool::entry                CMongoConnectionShp;

class CLctMongoConnPool final
{
   friend class CLctMongoHelper;
private:
   LCT_ERR_CODE init();

   LCT_ERR_CODE getConnection(CMongoConnectionShp& connectionShp);

   LCT_ERR_CODE tryGetConnection(CMongoConnectionShp& connectionShp);

private:
   explicit CLctMongoConnPool(const int32_t port, const std::string& ip, const int32_t connCount);

   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctMongoConnPool);

private:
   bool                   m_initiated   = false;
   const int32_t          m_port;
   const int32_t          m_connCount;
   const std::string      m_ip;

   typedef std::unique_ptr<CMongoPool> CMongoPoolShp;
   CMongoPoolShp          m_mongoPool   = nullptr;
};

typedef std::shared_ptr<CLctMongoConnPool> CLctMongoConnPoolShp;

class CLctMongoHelper final: public CLctSingleton<CLctMongoHelper>
{
public:
   LCT_ERR_CODE setConnCount(const int32_t connMaxCount);

   LCT_ERR_CODE countCollection(
         const int32_t mongoDbPort,
         const std::string& mongoDbIp,
         const std::string& dbNm,
         const std::string& tableNm,
         int64_t& count);

   template <typename CViewType>
   LCT_ERR_CODE countCollection(
         const int32_t mongoDbPort,
         const std::string& mongoDbIp,
         const std::string& dbNm,
         const std::string& tableNm,
         const CViewType& filterView,
         int64_t& count);

   LCT_ERR_CODE listCollectionName(
         const int32_t mongoDbPort,
         const std::string& mongoDbIp,
         const std::string& dbName,
         std::vector<std::string>& collectionNames);

   LCT_ERR_CODE listDatabaseName(
         const int32_t mongoDbPort,
         const std::string& mongoDbIp,
         std::vector<std::string>& tableNames);

   template <typename CModelType>
   LCT_ERR_CODE queryCollection(typename std::shared_ptr<CModelType>& modelShp);

   template <typename CModelType>
   LCT_ERR_CODE queryCollection(
         typename std::shared_ptr<CModelType>& modelShp,
         const int32_t offset,
         const int32_t batchSize);

   template <typename CModelType>
   LCT_ERR_CODE queryCollection(
         typename std::shared_ptr<CModelType>& modelShp,
         const int32_t offset,
         const int32_t batchSize,
         const CMongoFilter& filterExColumns);

   template <typename CModelType, typename CViewType>
   LCT_ERR_CODE queryOnFilter(const CViewType& filterView, typename std::shared_ptr<CModelType>& modelShp);

   template <typename CModelType, typename CViewType>
   LCT_ERR_CODE queryOnFilter(const CViewType& filterView,
         const int32_t offset,
         const int32_t batchSize,
         typename std::shared_ptr<CModelType>& modelShp);

   template <typename CModelType>
   LCT_ERR_CODE add2Collection(const typename std::shared_ptr<CModelType>& modelShp);

   template <typename CModelType>
   LCT_ERR_CODE update2Collection(const typename std::shared_ptr<CModelType>& modelShp, const bool upsert = true);

   template <typename CModelType>
   LCT_ERR_CODE createIndex2Collection(const typename std::shared_ptr<CModelType>& modelShp);

   template <typename CModelType>
   LCT_ERR_CODE removeFromCollection(const typename std::shared_ptr<CModelType>& modelShp);

private:
   LCT_ERR_CODE getMongoConnPool(const int32_t port, const std::string& ip, CLctMongoConnPoolShp& connPoolShp) const;
   LCT_ERR_CODE addMongoConnPool(const int32_t port, const std::string& ip);
   LCT_ERR_CODE retrieveMongoConnPool(const int32_t port, const std::string& ip, CLctMongoConnPoolShp& connPoolShp);
   LCT_ERR_CODE insertOne(const CMongoDocument& doc, CMongoCollection& collection);
   LCT_ERR_CODE insertBulk(const std::vector<CMongoDocument>& docVec, CMongoCollection& collection);
   LCT_ERR_CODE deleteOne(const CMongoDocument& doc, CMongoCollection& collection);
   LCT_ERR_CODE deleteMany(const std::vector<CMongoDocument>& filterVec, CMongoCollection& collection);
   LCT_ERR_CODE deleteAll(CMongoCollection& collection);

private:
   friend class CLctSingleton;

   explicit CLctMongoHelper();
   ~CLctMongoHelper();

   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctMongoHelper);

private:
   struct CMongoDriverStamp
   {
      CMongoDriverStamp(const int32_t port, const std::string& ip);

      int32_t    Port;
      std::string Ip;

      bool operator==(const CMongoDriverStamp& that) const;
      bool operator< (const CMongoDriverStamp& that) const;
   };

   typedef std::map<CMongoDriverStamp, CLctMongoConnPoolShp>  CLctMongoConnPoolContainer;
   CLctMongoConnPoolContainer  m_connPoolContainer;

   typedef CLctRwlockGuard    CConnPoolLockGuard;
   typedef CLctRwLock         CConnPoolLock;
   mutable CConnPoolLock      m_rwlock;

   constexpr static int32_t   DEFAULT_CONN_MAX_COUNT = 16;

   int32_t                    m_connMaxCount       = DEFAULT_CONN_MAX_COUNT;
   CMongoInstance             m_instance;
};

#define LCT_MONGO_HELPER CLctMongoHelper::Instance()

#include "detail/lct_mongo_helper.inl"
#include "detail/lct_mongo_helper.hpp"

#endif /* SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_H_ */
