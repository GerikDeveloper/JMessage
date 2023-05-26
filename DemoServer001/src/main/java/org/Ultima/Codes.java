package org.Ultima;

/***
 * Command Codes for Server version Demo_0.0.1
 ***/
public class Codes {
    public static final byte VERSION = 1;
    public static final byte BASIC_OK = 0;
    public static final byte BASIC_ERR = 1;
    public static final byte BASIC_YES = 2;
    public static final byte BASIC_NO = 3;
    public static final byte BASIC_PING = 5;
    public static final byte BASIC_DISCONNECT = 6;
    public static final byte BASIC_GET_SERVER_INFO = 7;
    public static final byte BASIC_GET_IP = 8;
    public static final byte BASIC_GET_DATE = 9;
    public static final byte ACCOUNT_REG = 10;
    public static final byte ACCOUNT_LOGIN = 11;
    public static final byte ACCOUNT_LOGOUT = 12;
    public static final byte ACCOUNT_WILL_BE_AUTHORIZED = 15;
    public static final byte REG_RESULT_ERR_ICN_TAKEN = 20;
    public static final byte REG_RESULT_ERR_EMAIL_TAKEN = 21;
    public static final byte UNF_GET_ID_BY_ICN = 30;
    public static final byte UNF_GET_ICN = 31;
    public static final byte UNF_GET_INFO = 32;
    public static final byte UNF_OK_FOUND = 35;
    public static final byte UNF_ERR_NOT_FOUND = 36;
    public static final byte GR_CREATE_GROUP = 40;
    public static final byte GR_ADD_MEMBER = 41;
    public static final byte GR_REMOVE_MEMBER = 42;
    public static final byte GR_GET_MEMBER_STATUS = 43;
    public static final byte GR_GET_MEMBERS = 44;
    public static final byte GR_RESULT_ERR_ICN_TAKEN = 45;
    public static final byte GR_GET_ID = 46;
    public static final byte GR_GET_ICN = 47;
    public static final byte GR_ERR_NOT_FOUND = 48;
    public static final byte GR_OK_FOUND = 49;
    public static final byte SM_SEND_MSG = 50;
    public static final byte SM_OK_CAN_SEND = 55;
    public static final byte SM_ERR_CAN_NOT_SEND = 56;
    public static final byte SM_MSG_WAS_SENT = 57;
    public static final byte SM_MSG_WAS_NOT_SENT = 58;
    public static final byte GM_GET_MSG_IDS = 60;
    public static final byte GM_GET_MSG = 65;
    public static final byte GM_ERR_NO_ACCESS = 70;
    public static final byte GM_ERR_NO_SUCH_MSG = 71;
    public static final byte GM_ERR_UNKNOWN_MSG_TYPE = 72;
    public static final byte GM_OK = 73;
    public static final byte MSG_TYPE_TEXT = 100;
    public static final byte MSG_TYPE_FILE = 101;
}
