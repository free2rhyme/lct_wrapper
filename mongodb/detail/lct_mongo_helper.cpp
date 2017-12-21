/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_mongo_client_helper.cpp
 * @version     1.0
 * @date        May 25, 2017 7:14:50 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#include <sstream>
#include "lct_log.h"
#include "lct_mongo_helper.h"
#include "lct_cplusplus_14.h"

LCT_ERR_CODE CLctMongoConnPool::init()
{
   if (m_initiated || (m_mongoPool != nullptr)) {
      return LCT_FAIL;
   }
   try {
      std::stringstream ss;
      ss << "mongodb://" << m_ip << ":" << m_port;
      ss << "/?minPoolSize=" << m_connCount << "&maxPoolSize=" << m_connCount;

      const auto mgUrl = mongocxx::uri(ss.str());

      m_mongoPool = std::make_unique<CMongoPool>(mgUrl);
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
       LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   m_initiated = true;
   return LCT_SUCCESS;
}

LCT_ERR_CODE CLctMongoConnPool::getConnection(CMongoConnectionShp& connectionShp)
{
   if (!m_initiated || (m_mongoPool == nullptr)) {
      LOG_ERROR << ErrCodeFormat(LCT_FAIL);
      return LCT_FAIL;
   }
   try {
      connectionShp = std::move(m_mongoPool->acquire());
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}

LCT_ERR_CODE CLctMongoConnPool::tryGetConnection(CMongoConnectionShp& connectionShp)
{
   if (!m_initiated || (m_mongoPool == nullptr)) {
      LOG_ERROR << ErrCodeFormat(LCT_FAIL);
      return LCT_FAIL;
   }
   try {
      auto&& optionalConn = m_mongoPool->try_acquire();
      if (!optionalConn) {
         LOG_WARNG << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
      connectionShp = std::move(*optionalConn);
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}

CLctMongoConnPool::CLctMongoConnPool(const int32_t port, const std::string& ip, const int32_t connCount):
   m_port(port), m_connCount(connCount), m_ip(ip)
{
}

LCT_ERR_CODE CLctMongoHelper::countCollection(
      const int32_t mongoDbPort,
      const std::string& mongoDbIp,
      const std::string& dbNm,
      const std::string& tableNm,
      int64_t& count)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(mongoDbPort, mongoDbIp, connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      CMongoConnectionShp connectionShp;
      errCode = connPoolShp->getConnection(connectionShp);
      if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }
      auto&& db      = (*connectionShp)[dbNm];
      auto&& table    = db[tableNm];

      count = table.count({});

   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::listCollectionName(
      const int32_t mongoDbPort,
      const std::string& mongoDbIp,
      const std::string& dbName,
      std::vector<std::string>& collectionNames)
{

   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(mongoDbPort, mongoDbIp, connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      CMongoConnectionShp connectionShp;
      errCode = connPoolShp->getConnection(connectionShp);
      if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      auto&& dbInstance = (*connectionShp)[dbName];
      auto&& tbCursor = dbInstance.list_collections();
      for (const auto& tt: tbCursor) {
         auto tbNm = tt.find("name");
         if (tbNm != tt.end()) {
            collectionNames.push_back(tbNm->get_utf8().value.to_string());
         }
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::listDatabaseName(
      const int32_t mongoDbPort,
      const std::string& mongoDbIp,
      std::vector<std::string>& tableNames)
{

   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(mongoDbPort, mongoDbIp, connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {

      CMongoConnectionShp connectionShp;
      errCode = connPoolShp->getConnection(connectionShp);
      if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      auto&& cursor = connectionShp->list_databases();
      for (const auto& cc: cursor) {
         std::vector<std::string> db;
         auto itRe = cc.find("name");
         if (itRe != cc.end()) {
            tableNames.push_back(itRe->get_utf8().value.to_string());
         }
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::getMongoConnPool(const int32_t port, const std::string& ip, CLctMongoConnPoolShp& connPoolShp) const
{
   CConnPoolLockGuard lk(m_rwlock);
   CLctMongoConnPoolContainer::const_iterator itRe = m_connPoolContainer.find(CMongoDriverStamp(port, ip));
   if (itRe == m_connPoolContainer.end()) {
      return LCT_EMPTY_RECORD;
   }
   connPoolShp = itRe->second;
   return LCT_SUCCESS;
}

LCT_ERR_CODE CLctMongoHelper::addMongoConnPool(const int32_t port, const std::string& ip)
{
   CConnPoolLockGuard lk(m_rwlock, true);
   CLctMongoConnPoolContainer::const_iterator itRe = m_connPoolContainer.find(CMongoDriverStamp(port, ip));
   if (itRe != m_connPoolContainer.end()) {
      LOG_ERROR << ErrCodeFormat(LCT_FAIL);
      return LCT_FAIL;
   }

   CLctMongoConnPoolShp connPoolShp(new CLctMongoConnPool(port, ip, m_connMaxCount));
   LCT_ERR_CODE errCode = connPoolShp->init();
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   m_connPoolContainer.insert(std::pair<CMongoDriverStamp, CLctMongoConnPoolShp>(CMongoDriverStamp(port, ip), connPoolShp));
   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::retrieveMongoConnPool(const int32_t port, const std::string& ip, CLctMongoConnPoolShp& connPoolShp)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;
   errCode = getMongoConnPool(port, ip, connPoolShp);
   if (LCT_SUCCESS != errCode) {
      errCode = addMongoConnPool(port, ip);
      if (LCT_SUCCESS != errCode) {
         LOG_WARNG << ErrCodeFormat(errCode);
      }
      errCode = getMongoConnPool(port, ip, connPoolShp);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }
   }
   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::insertOne(const CMongoDocument& doc, CMongoCollection& collection)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   try {
      const auto& result = collection.insert_one(std::move(doc.view()));
      if (!result) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::insertBulk(const std::vector<CMongoDocument>& docVec, CMongoCollection& collection)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   try {
      CMongoBulkWrite bulk{};
      for (const auto& doc: docVec) {
         CMongoInsertOne insertOp{doc.view()};
         bulk.append(insertOp);
      }
      const auto& result = collection.bulk_write(std::move(bulk));
      if (!result) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}


LCT_ERR_CODE CLctMongoHelper::deleteOne(const CMongoDocument& doc, CMongoCollection& collection)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   try{
      const auto& result = collection.delete_many(std::move(doc.view()));
      if (!result) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::deleteMany(const std::vector<CMongoDocument>& filterVec, CMongoCollection& collection)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   try {
      CMongoDocument filter{};
      CMongoArray FilterArray{};
      for (const auto& doc: filterVec) {
         FilterArray << doc.view();
      }
      filter << "$or" << FilterArray.view();

      const auto& result = collection.delete_many(std::move(filter.view()));
      if (!result) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}

LCT_ERR_CODE CLctMongoHelper::deleteAll(CMongoCollection& collection)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   try{
      const auto& result = collection.delete_many({});
      if (!result) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return errCode;
}

CLctMongoHelper::CLctMongoHelper()
{
}

CLctMongoHelper::~CLctMongoHelper()
{
}

CLctMongoHelper::CMongoDriverStamp::CMongoDriverStamp(const int32_t port, const std::string& ip):
   Port(port), Ip(ip)
{
}

