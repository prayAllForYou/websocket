#!/usr/bin/python
# encoding:utf-8
"""
overlord api ERROR_CODE Constants
"""

# global
NORMAL = 0
COOKIE_EXPIRED = 1
API_INTERNAL_ERROR = 2
ACCESS_RIGHT_ERROR = 3
UNKNOWN_ERROR = 10
PARAMETERS_INVALID = 11
IP_NOT_IN_SETTING_LIST = 12
MULTIPLE_LOGIN = 13
SOURCE_CHECK_FAILED = 14
SQL_EXCUTE_FAILER = 15
DEVICE_CHECK_FAILED = 17
MOBILE_BIND_FIRST = 18
MOBILE_MSG_VERIFY_FIRST = 19

# account
INVALID_ACCOUNT_OR_PASSWORD = 1002
USER_ON_TRIAL = 1003
USER_FORBIDDEN = 1004
NO_AUTH_INFO = 1004
AGENT_NOT_FOUND = 1005
INVALID_VIDEO_PASSWORD = 1006
INVALID_MAIL = 1007
ACTIVE_CODE_EXPIRE = 1008
USER_NOT_EXISTS = 1009
USER_DOMAIN_ERROR = 1010
CANNOT_CREATE_SUB_ADMIN = 1011
REACH_STAFF_LIMIT = 1012
NEED_ACTIVATE = 1013
NEED_RESET_ACTIVATE = 1014
NO_NEED_TO_ACTIVATE = 1015
NAME_INVALID_CHAR = 1016
EXCEED_RETRY_LIMIT = 1017
VERIFY_CODE_ERROR = 1018
VERIFY_TOKEN_ERROR = 1019
OPERATION_TOO_FREQUENT = 1020
MOBILE_EXISTS = 1023
SMS_SEND_CODE_WRONG = 1024
PHONE_VERIFY_LIMIT = 1025
CAPTCHA_HOUR_LIMIT = 1026
VERIFY_PWD_ERROR = 1027
PWD_VERIFY_LIMIT = 1028

# sub account
SUB_ID_REQUIRE = 8001
DELETE_USER_FAILED = 8002
SUB_USERNAME_EXISTS = 8003

# group
GROUP_ID_REQUIRE = 12001
GROUP_NAME_EXIST = 12002
DELETE_GROUP_FAILED = 12003
GROUP_NOT_FOUND = 12004
GROUP_NAME_REQUIRE = 12005
GROUP_USER_LIST_INVALID = 12006
GROUP_WORK_TABLES_INVALID = 12007

# enterprise
ENTERPRISE_NOT_EXISTS = 12008
ENTERPRISE_EXISTING = 12009
INVITATION_CODE_ERROR = 12010
INVITATION_CODE_PERMISSION_ERROR = 12011

# personal
PERSONAL_USER_EXSITS = 23001
PERSONAL_VERIFY_CODE_WRONG = 23002
PERSONAL_USER_NOT_EXSITS = 23003
PERSONAL_SMS_TRY_LIMIT = 23006
PERSONAL_INVITATION_CODE_WRONG = 23007
PERSONAL_NICK_NAME_EXSITS = 23008
PERSONAL_MODIFY_EMAIL_WRONG = 23009
PERSONAL_SMS_VERIFY_LIMIT = 23010
PERSONAL_SMS_GEN_CODE_WRONG = 23011
PERSONAL_SMS_SEND_CODE_WRONG = 23012
PERSONAL_INVALID_PHONE_NUMBER = 23013
PERSONAL_IP_INVALID_ONE_HOUR = 23014
PERSONAL_INVALID_EMAIL = 23015
PERSONAL_IP_INVALID_ONE_DAY = 23016
PERSONAL_MODIFY_EMAIL_LIMIT = 23017
PERSONAL_MODIFY_MOBILE_WRONG = 23018
PERSONAL_EMAIL_EXISTS = 23019
PERSONAL_PHONE_EXISTS = 23020
PERSONAL_INVITATION_CODE_USED = 23021
PERSONAL_INVITATION_CODE_REQUEST_LIMIT = 23022
