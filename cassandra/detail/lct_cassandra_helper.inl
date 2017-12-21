/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cassandra_helper.inl
 * @version     1.0
 * @date        Oct 10, 2017 5:35:27 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/
#ifndef SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASSANDRA_HELPER_INL_
#define SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASSANDRA_HELPER_INL_

inline const char* CLctCassandraHelper::cassErrDesc(const CassError err) const
{
   return cass_error_desc(err);
}

inline LCT_ERR_CODE CLctCassandraHelper::setChunkSize(const std::size_t size)
{   
   if (size <= 0) {
      return LCT_FAIL;
   }
   m_batchExecChunkSize = size;
   return LCT_SUCCESS;
}

inline std::size_t CLctCassandraHelper::chunkSize() const
{
   return m_batchExecChunkSize;
}

inline LCT_ERR_CODE CLctCassandraHelper::setPageQuerySize(const std::size_t size)
{
    if (size <= 0) {
        return LCT_FAIL;
    }
    m_pageQuerySize = size;
    return LCT_SUCCESS;
}

inline std::size_t CLctCassandraHelper::pageQuerySize() const
{
    return m_pageQuerySize;
}

#endif /* SRC_WRAPPER_CASSANDRA_DETAIL_LCT_CASSANDRA_HELPER_INL_ */
