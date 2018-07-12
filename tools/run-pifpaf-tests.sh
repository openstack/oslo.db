#!/bin/bash
set -e
# Replace mysql:// by mysql+pymysql:// and add sqlite
export OS_TEST_DBAPI_ADMIN_CONNECTION="${OS_TEST_DBAPI_ADMIN_CONNECTION/#mysql:/mysql+pymysql:};sqlite://"
echo $OS_TEST_DBAPI_ADMIN_CONNECTION
stestr run $*
TEST_EVENTLET=1 stestr run $*
