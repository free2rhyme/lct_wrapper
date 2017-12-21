/**********************************************************************
 * @copyright   Copyright (C), 2017
 * @file        lct_cass_access_restrain.cpp
 * @version     1.0
 * @date        Nov 2, 2017 2:42:39 PM
 * @author      wlc2rhyme@gmail.com
 * @brief       TODO
 *********************************************************************/

#include "lct_cass_access_restrain.h"

std::size_t               CCassRestrain::s_concurrent     = 0;
std::size_t               CCassRestrain::s_limit          = CASSANDRA_MAX_LIMIT_DEFAULT_SIZE;
std::condition_variable   CCassRestrain::s_cond;
std::mutex                CCassRestrain::s_mutex;
std::size_t               CCassRestrain::s_waitingMaxTime = CASSANDRA_RESTRAIN_WAITING_SECONDS;


