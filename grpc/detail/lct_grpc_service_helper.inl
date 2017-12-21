/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_grpc_client_helper.inl
 * @version     1.0
 * @date        May 17, 2017 6:42:45 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_INL_
#define SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_INL_

// Number of server completion queues to create to listen to incoming RPCs.
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setCqNumber(const int32_t count)
{
   if (count <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_cqNumber = count;
   return LCT_SUCCESS;
}

// Minimum number of threads per completion queue that should be listening
// to incoming RPCs.
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setCqMinPoller(const int32_t count)
{
   if (count <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_cqMinPollers = count;
   return LCT_SUCCESS;
}

// Maximum number of threads per completion queue that should be listening
// to incoming RPCs.
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setCqMaxPoller(const int32_t count)
{
   if (count <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_cqMaxPollers = count;
   return LCT_SUCCESS;
}

// The timeout for server completion queue's AsyncNext call.
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setTimeout(const int32_t msec)
{
   if (msec <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_timeoutMsec = msec;
   return LCT_SUCCESS;
}

/** After a duration of this time the client/server pings its peer to see if the
   transport is still alive. Int valued, milliseconds. */
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setKeepaliveInterval(const int32_t msec)
{
   if (msec <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_keepaliveInterval = msec;
   return LCT_SUCCESS;
}

/** After waiting for a duration of this time, if the keepalive ping sender does
   not receive the ping ack, it will close the transport. Int valued,
   milliseconds. */
template <typename CServiceImpl>
inline LCT_ERR_CODE CLctGRpcService<CServiceImpl>::setKeepaliveTimeout(const int32_t msec)
{
   if (msec <= 0) {
      return LCT_INVALID_PARAM;
   }

   m_keepaliveTimeout = msec;
   return LCT_SUCCESS;
}

template <typename CServiceImpl>
inline uint16_t CLctGRpcService<CServiceImpl>::getPort() const
{
   return m_servicePort;
}

template <typename CServiceImpl>
inline std::string CLctGRpcService<CServiceImpl>::getIp() const
{
   return m_serviceIp;
}

template <typename CServiceImpl>
inline bool CLctGRpcService<CServiceImpl>::isServing() const
{
   return m_isServing;
}

#endif /* SRC_WRAPPER_GRPC_LCT_GRPC_SERVICE_HELPER_INL_ */
