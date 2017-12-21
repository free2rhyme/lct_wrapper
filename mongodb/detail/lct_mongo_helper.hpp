/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_mongo_client_helper.hpp
 * @version     1.0
 * @date        Jun 23, 2017 4:07:17 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_HPP_
#define SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_HPP_

#include "lct_log.h"

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::queryCollection(typename std::shared_ptr<CModelType>& modelShp)
{
   return queryCollection(modelShp, 0, -1, CMongoFilter());
}

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::queryCollection(
      typename std::shared_ptr<CModelType>& modelShp,
      const int32_t offset,
      const int32_t batchSize)
{
   return queryCollection(modelShp, offset, batchSize, CMongoFilter());
}

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::queryCollection(
      typename std::shared_ptr<CModelType>& modelShp,
      const int32_t offset,
      const int32_t batchSize,
      const CMongoFilter& filterExColumns)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];

      CMongoFindOption   filter;
      CMongoCountOption   countFilter;
      if (offset >= 0) {
         filter.skip(offset);
         countFilter.skip(offset);
      }

      if (batchSize != -1) {
         filter.limit(batchSize);
         countFilter.limit(batchSize);
      }

      CMongoDocument filterBuilder;
      if (!filterExColumns.empty()) {
         for (const auto& ref:filterExColumns) {
            filterBuilder << ref << 0;
         }
      }
      filter.projection(filterBuilder.view());

      errCode = modelShp->Assign(table.count({}, countFilter), table.find({}, filter));
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
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

template <typename CModelType, typename CViewType>
LCT_ERR_CODE CLctMongoHelper::queryOnFilter(const CViewType& filterBuilder, typename std::shared_ptr<CModelType>& modelShp)
{
   return queryOnFilter(filterBuilder, 0, -1, modelShp);
}


template <typename CModelType, typename CViewType>
LCT_ERR_CODE CLctMongoHelper::queryOnFilter(const CViewType& filterBuilder,
      const int32_t offset,
      const int32_t batchSize,
      typename std::shared_ptr<CModelType>& modelShp)
{
  LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];


      CMongoFindOption   filter;
      CMongoCountOption   countFilter;
      if (offset >= 0) {
         filter.skip(offset);
         countFilter.skip(offset);
      }

      if (batchSize != -1) {
         filter.limit(batchSize);
         countFilter.limit(batchSize);
      }

      errCode = modelShp->Assign(table.count(filterBuilder.view(), countFilter), table.find(filterBuilder.view(), filter));
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
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

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::add2Collection(const typename std::shared_ptr<CModelType>& modelShp)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];

      std::vector<CMongoDocument> docVector;
      errCode = modelShp->Fetch(docVector);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      if (docVector.size() > 1) {
         return InsertBulk(docVector, table);
      } else if (docVector.size() == 1) {
         return InsertOne(*(docVector.cbegin()), table);
      } else {
         LOG_WARNG << "There is none record to save";
         return LCT_SUCCESS;
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << ") Exception occurred(" << e.what() << ")";
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

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::update2Collection(const typename std::shared_ptr<CModelType>& modelShp, const bool upsert)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];

      std::vector<CMongoDocument> filterVector;
      std::vector<CMongoDocument> docVector;
      errCode = modelShp->FetchOnFilter(filterVector, docVector);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      const std::size_t size = std::min(filterVector.size(), docVector.size());
      for (std::size_t i = 0; i < size; ++i) {
         const auto& filter = filterVector.at(i);
         const auto& doc   = docVector.at(i);

         LOG_DEBUG << bsoncxx::to_json(filter);
         LOG_DEBUG << bsoncxx::to_json(doc);

         CMongoUpdateOption options;
         options.upsert(upsert);

         const auto& result = table.update_one(std::move(filter.view()), std::move(doc.view()), options);
         if (!result) {
            LOG_ERROR << ErrCodeFormat(LCT_FAIL);
            return LCT_FAIL;
         }
      }
   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << ") Exception occurred(" << e.what() << ")";
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


template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::createIndex2Collection(const typename std::shared_ptr<CModelType>& modelShp)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try {
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];

      CMongoDocument doc{};
      errCode = modelShp->Fetch2Index(doc);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      CMongoIndex indexOptions{};
      indexOptions.unique(true);

      table.create_index(std::move(doc.view()), std::move(indexOptions));

   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << ") Exception occurred(" << e.what() << ")";
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

template <typename CModelType>
LCT_ERR_CODE CLctMongoHelper::removeFromCollection(const typename std::shared_ptr<CModelType>& modelShp)
{
   LCT_ERR_CODE errCode = LCT_SUCCESS;

   CLctMongoConnPoolShp connPoolShp;
   errCode = retrieveMongoConnPool(modelShp->DatabasePort(), modelShp->DatabaseIp(), connPoolShp);
   if (LCT_SUCCESS != errCode) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   CMongoConnectionShp connectionShp;
   errCode = connPoolShp->getConnection(connectionShp);
   if (LCT_SUCCESS != errCode || (connectionShp == nullptr)) {
      LOG_ERROR << ErrCodeFormat(errCode);
      return errCode;
   }

   try{
      auto&& db    = (*connectionShp)[modelShp->DatabaseNm()];
      auto&& table  = db[modelShp->TableNm()];

      std::vector<CMongoDocument> docVector;
      errCode = modelShp->Filter(docVector);
      if (LCT_SUCCESS != errCode) {
         LOG_ERROR << ErrCodeFormat(errCode);
         return errCode;
      }

      if (docVector.size() > 1) {
         return DeleteMany(docVector, table);
      } else if (docVector.size() == 1) {
         return DeleteOne(*(docVector.cbegin()), table);
      } else {
         return DeleteAll(table);
      }

   } catch (const CMongoException& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << ") Exception occurred(" << e.what() << ")";
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

template <typename CViewType>
LCT_ERR_CODE CLctMongoHelper::countCollection(
      const int32_t mongoDbPort,
      const std::string& mongoDbIp,
      const std::string& dbNm,
      const std::string& tableNm,
      const CViewType& filterBuilder,
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
      auto db      = (*connectionShp)[dbNm];
      auto table   = db[tableNm];

      count = table.count({filterBuilder.view()});

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


#endif /* SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_HPP_ */
