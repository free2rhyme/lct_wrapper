/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_mongo_client_helper.inl
 * @version     1.0
 * @date        Jun 23, 2017 3:51:40 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#ifndef SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_INL_
#define SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_INL_

inline LCT_ERR_CODE CLctMongoHelper::setConnCount(const int32_t connMaxCount)
{
   m_connMaxCount = connMaxCount;
   return LCT_SUCCESS;
}

inline bool CLctMongoHelper::CMongoDriverStamp::operator==(const CMongoDriverStamp& that) const
{
   return ((Port == that.Port) && (Ip == that.Ip));
}

inline bool CLctMongoHelper::CMongoDriverStamp::operator<(const CMongoDriverStamp& that) const
{
   if (Ip < that.Ip) {
      return true;
   }
   if (Ip > that.Ip) {
      return false;
   }
   if (Port < that.Port) {
      return true;
   } else {
      return false;
   }
}

#endif /* SRC_WRAPPER_MONGODB_LCT_MONGO_HELPER_INL_ */

