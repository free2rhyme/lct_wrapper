/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_grpc_client_helper.hpp
 * @version     1.0
 * @date        May 17, 2017 6:42:45 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_HPP_
#define SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_HPP_

#include <memory>
#include "lct_log.h"

template <typename CServiceImpl>
CLctGRpcService<CServiceImpl>::CLctGRpcService(const uint16_t svcPort, const std::string& svcIp):
   m_isServing(false), m_servicePort(svcPort), m_serviceIp(svcIp)
{
}

template <typename CServiceImpl>
CLctGRpcService<CServiceImpl>::~CLctGRpcService()
{
   shutdown();
}

template <typename CServiceImpl>
LCT_ERR_CODE CLctGRpcService<CServiceImpl>::init()
{
   const std::string svcAddr = m_serviceIp + ":" + std::to_string(m_servicePort);

   try{
      m_serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, m_keepaliveInterval);

      m_serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, m_keepaliveTimeout);

      // Number of server completion queues to create to listen to incoming RPCs.
      m_serverBuilder.SetSyncServerOption(GRpcServerBuilder::SyncServerOption::NUM_CQS, m_cqNumber);

      // Minimum number of threads per completion queue that should be listening
      // to incoming RPCs.
      m_serverBuilder.SetSyncServerOption(GRpcServerBuilder::SyncServerOption::MIN_POLLERS, m_cqMinPollers);

      // Maximum number of threads per completion queue that should be listening
      // to incoming RPCs.
      m_serverBuilder.SetSyncServerOption(GRpcServerBuilder::SyncServerOption::MAX_POLLERS, m_cqMaxPollers);

      // The timeout for server completion queue's AsyncNext call.
      m_serverBuilder.SetSyncServerOption(GRpcServerBuilder::SyncServerOption::CQ_TIMEOUT_MSEC, m_timeoutMsec);

      if (0 == m_servicePort) {
         m_serverBuilder.AddListeningPort(svcAddr, grpc::InsecureServerCredentials(), &m_selectedPort);
      } else {
         m_serverBuilder.AddListeningPort(svcAddr, grpc::InsecureServerCredentials());
      }

      m_svcImplShp.reset(new CServiceImpl());

      m_serverBuilder.RegisterService(m_svcImplShp.get());

   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_EXCEPTION) << " e.what(" << e.what() << ")";
      return LCT_EXCEPTION;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }

   return LCT_SUCCESS;
}

template <typename CServiceImpl>
LCT_ERR_CODE CLctGRpcService<CServiceImpl>::serve(bool syncWait /* = true */)
{
   try {
      m_serverUnp = m_serverBuilder.BuildAndStart();
      if (!m_serverUnp) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
      m_isServing = true;
      if (syncWait) {
         m_serverUnp->Wait();
      }
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_EXCEPTION) << " e.what(" << e.what() << ")";
      return LCT_EXCEPTION;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}

template <typename CServiceImpl>
LCT_ERR_CODE CLctGRpcService<CServiceImpl>::serve(OnPortUpdatedFunc fn)
{
   try{
      m_serverUnp = m_serverBuilder.BuildAndStart();
      if (!m_serverUnp) {
         LOG_ERROR << ErrCodeFormat(LCT_FAIL);
         return LCT_FAIL;
      }
      if ((0 == m_servicePort) && (0 != m_selectedPort)) {
         m_servicePort = m_selectedPort;
         fn(m_servicePort);
      }

      m_isServing = true;
      m_serverUnp->Wait();

   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_EXCEPTION) << " e.what(" << e.what() << ")";
      return LCT_EXCEPTION;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(IVA_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}

template <typename CServiceImpl>
LCT_ERR_CODE CLctGRpcService<CServiceImpl>::shutdown()
{
   if (m_isServing) {
      try {
         if (m_serverUnp != nullptr) {
            m_serverUnp->Shutdown();
         }
      } catch (const std::exception& e) {
         LOG_ERROR << ErrCodeFormat(LCT_EXCEPTION) << " e.what(" << e.what() << ")";
         return LCT_EXCEPTION;
      } catch (...) {
         LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
         return LCT_UNEXPECTED;
      }
      m_isServing = false;
   }
   return LCT_SUCCESS;
}

template <typename CServiceImpl>
LCT_ERR_CODE CLctGRpcServiceHelper::Serve(const uint16_t svcPort, const std::string& svcIp, bool syncWait /* = true */)
{
   const std::string svcAddr = svcIp + ":" + std::to_string(svcPort);
   try {
      GRpcServerBuilder serverBuilder;
      serverBuilder.AddListeningPort(svcAddr, grpc::InsecureServerCredentials());

      std::unique_ptr<CServiceImpl> svcUnq(new CServiceImpl());
      serverBuilder.RegisterService(svcUnq.get());

      GRpcServerUnp serverUnp = serverBuilder.BuildAndStart();
      if (syncWait) {
         serverUnp->Wait();
      }
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_EXCEPTION) << " e.what(" << e.what() << ")";
      return LCT_EXCEPTION;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}

#endif /* SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_HPP_ */
