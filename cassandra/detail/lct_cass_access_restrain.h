/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_access_restrain.h
 * @version     1.0
 * @date        Nov 2, 2017 2:25:41 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_ACCESS_RESTRAIN_H_
#define SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_ACCESS_RESTRAIN_H_

#include "lct_cass_common_define.h"
#include <mutex>
#include <condition_variable>

class CCassRestrain final
{
public:
   explicit CCassRestrain()
   {
      std::unique_lock<std::mutex> lk(s_mutex);
      while (s_concurrent >= s_limit) {
         s_cond.wait_for(lk, std::chrono::seconds(s_waitingMaxTime));
      }
      ++s_concurrent;
   }

   ~CCassRestrain()
   {
      std::lock_guard<std::mutex> lk(s_mutex);
      --s_concurrent;
      s_cond.notify_all();
   }

   static std::size_t limit()
   {
      return s_limit;
   }

   static void close()
   {
      std::lock_guard<std::mutex> lk(s_mutex);
      s_cond.notify_all();
   }

   static void setConcurrentMax(const std::size_t max)
   {
      s_limit = max;
   }

   static void setWaitingTime(const std::size_t seconds)
   {
      s_waitingMaxTime = seconds;
   }

private:
   static std::size_t              s_concurrent;
   static std::size_t              s_limit;
   static std::condition_variable  s_cond;
   static std::mutex               s_mutex;
   static std::size_t              s_waitingMaxTime;
};

#define CassAutoFlowControl() CCassRestrain c_cass_restrain_var

#endif /* SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_ACCESS_RESTRAIN_H_ */
