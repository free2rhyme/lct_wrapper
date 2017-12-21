/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_grpc_client_helper.h
 * @version     1.0
 * @date        May 17, 2017 6:42:45 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_H_
#define SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_H_

#include <string>
#include <grpc++/server.h>
#include "lct_common_define.h"

typedef grpc::Server           GRpcServer;
typedef grpc::ServerBuilder    GRpcServerBuilder;
typedef grpc::ServerContext    GRpcServerContext;
typedef grpc::Status           GRpcStatus;

typedef std::unique_ptr<GRpcServer>  GRpcServerUnp;

typedef std::function<void(const uint16_t selectedPort)> OnPortUpdatedFunc;

template <typename CServiceImpl>
class CLctGRpcService
{
public:
   explicit CLctGRpcService(const uint16_t svcPort, const std::string& svcIp);

   virtual ~CLctGRpcService();

   // Number of server completion queues to create to listen to incoming RPCs.
   LCT_ERR_CODE setCqNumber(const int32_t count);

   // Minimum number of threads per completion queue that should be listening
   // to incoming RPCs.
   LCT_ERR_CODE setCqMinPoller(const int32_t count);

   // Maximum number of threads per completion queue that should be listening
   // to incoming RPCs.
   LCT_ERR_CODE setCqMaxPoller(const int32_t count);

   // The timeout for server completion queue's AsyncNext call.
   LCT_ERR_CODE setTimeout(const int32_t msec);

   /** After a duration of this time the client/server pings its peer to see if the
      transport is still alive. Int valued, milliseconds. */
   LCT_ERR_CODE setKeepaliveInterval(const int32_t msec);

   /** After waiting for a duration of this time, if the keepalive ping sender does
      not receive the ping ack, it will close the transport. Int valued,
      milliseconds. */
   LCT_ERR_CODE setKeepaliveTimeout(const int32_t msec);

   LCT_ERR_CODE init();

   LCT_ERR_CODE serve(bool syncWait = true);

   LCT_ERR_CODE serve(OnPortUpdatedFunc fn);

   LCT_ERR_CODE shutdown();

   uint16_t getPort() const;

   std::string getIp() const;

   bool isServing() const;

private:
   bool                m_isServing;
   uint16_t            m_servicePort;
   int32_t             m_selectedPort      = 0;
   int32_t             m_cqNumber          = 1;
   int32_t             m_cqMinPollers      = 1;
   int32_t             m_cqMaxPollers      = std::numeric_limits<int32_t>::max();
   int32_t             m_timeoutMsec       = 1000; //milliseconds
   int32_t             m_keepaliveInterval = 1000 * 10; //milliseconds
   int32_t             m_keepaliveTimeout  = 1000 * 2; //milliseconds

   std::string         m_serviceIp;

private:
   typedef typename std::unique_ptr<CServiceImpl>   CLctSvcImplShp;

   GRpcServerUnp       m_serverUnp;
   CLctSvcImplShp      m_svcImplShp;
   GRpcServerBuilder   m_serverBuilder;

private:
   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctGRpcService);
};

class CLctGRpcServiceHelper final
{
public:
   template <typename CServiceImpl>
   static LCT_ERR_CODE Serve(const uint16_t svcPort, const std::string& svcIp, bool syncWait = true);

private:
   DISALLOW_INSTANTIATE(CLctGRpcServiceHelper);
   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctGRpcServiceHelper);
};

#include "detail/lct_grpc_service_helper.hpp"
#include "detail/lct_grpc_service_helper.inl"

#endif /* SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_H_ */
