/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_page_state_vessel.h
 * @version     1.0
 * @date        Oct 19, 2017 5:51:49 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_PAGE_STATE_VESSEL_H_
#define SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_PAGE_STATE_VESSEL_H_

#include "lct_common_define.h"
#include "lct_rw_lock.h"
#include "lct_cass_common_define.h"

class CLctCassPageStateVessel
{
   static constexpr int64_t PAGE_STATE_OBSOLETE_DURATION = 24 * 60 * 60 * 1000; // 24 hours

public:
   LCT_ERR_CODE save(const std::string& cql, const LctCassPageInfo& pageInfo);
   LCT_ERR_CODE update(const std::string& cql, const LctCassPageInfo& pageInfo);
   LCT_ERR_CODE remove(const std::string& cql);    
   LCT_ERR_CODE retrieve(const std::string& cql, LctCassPageInfo& pageInfo);

private:
   LCT_ERR_CODE tidy();

public:
   CLctCassPageStateVessel();
   ~CLctCassPageStateVessel();

   DISALLOW_COPY_MOVE_OR_ASSIGN(CLctCassPageStateVessel);

private:
   typedef std::unordered_map<std::string, LctCassPageInfo> CLctCassPageStateMap;
   CLctCassPageStateMap m_pageStateMap;

   mutable CLctRwLock  m_mutex;
};


#endif /* SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASS_PAGE_STATE_VESSEL_H_ */
