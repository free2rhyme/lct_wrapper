/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_grpc_client_helper.hpp
 * @version     1.0
 * @date        Jul 2, 2017 4:48:34 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_HPP_
#define SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_HPP_

#include "lct_log.h"
#include <grpc++/grpc++.h>

template <typename ClientType, typename ServiceCallable, class CRequestType, class CReplyType>
LCT_ERR_CODE CLctGRpcClientHelper::CallService(const uint16_t svcPort, const std::string& svcIp, ServiceCallable ServiceFunc, const CRequestType& request, CReplyType& reply)
{
   const std::string svcAddr = svcIp + ":" + std::to_string(svcPort);
   try {
      std::unique_ptr<typename ClientType::Stub> stub(
         ClientType::NewStub(grpc::CreateChannel(svcAddr, grpc::InsecureChannelCredentials())));

      grpc::ClientContext context;

      std::function<grpc::Status()> remoteCall = std::bind(ServiceFunc, std::ref(stub), &context, std::ref(request), &reply);

      grpc::Status status = remoteCall();

      if (status.ok()) {
         LOG_DEBUG << "Successful to call remote service";
         return LCT_SUCCESS;
      } else {
         LOG_ERROR << "Failed to call remote service error(" << status.error_message() << ")";
         return LCT_FAIL;
      }
   } catch (const std::exception& e) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED) << " e.what(" << e.what() << ")";
      return LCT_UNEXPECTED;
   } catch (...) {
      LOG_ERROR << ErrCodeFormat(LCT_UNEXPECTED);
      return LCT_UNEXPECTED;
   }
   return LCT_SUCCESS;
}


#endif /*SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_HPP_*/

