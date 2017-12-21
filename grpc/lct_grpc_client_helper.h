/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_grpc_client_helper.h
 * @version     1.0
 * @date        Jul 2, 2017 4:48:34 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_H_
#define SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_H_

#include "lct_common_define.h"

class CLctGRpcClientHelper final
{
public:
   template <typename ClientType, typename ServiceCallable, class CRequestType, class CReplyType>
   static LCT_ERR_CODE CallService(const uint16_t svcPort, const std::string& svcIp, ServiceCallable ServiceFunc, const CRequestType& request, CReplyType& reply);

private:
   DISALLOW_INSTANTIATE(CLctGRpcClientHelper);
   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctGRpcClientHelper);
};

#include "detail/lct_grpc_client_helper.hpp"

#endif /*SRC_WRAPPER_GRPC_LCT_GRPC_CLIENT_HELPER_H_*/

