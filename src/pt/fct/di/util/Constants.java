package pt.fct.di.util;

public class Constants {

	public static final String DEFAULT_DB_NAME = "pt.fct.di.db.Cassandra6";
	public static final String DEFAULT_COMM_CONF_FILE = "conf/comm/stackConf.xml";
	
	public static final String DEFAULT_DB_TABLE_NAME = "usertable";
	public static final String DEFAULT_DB_FAMILY_COLUMN = "data";
	
	public static final String DEFAULT_CONNECTION_RETRY_PROPERTY="1";
	public static final String DEFAULT_OPERATION_RETRY_PROPERTY="1";
	
	public static final String DEFAULT_SERVER_COMM_STACK = "pt.fct.di.serverProxy.comm.SimpleTCPComm";
	public static final String DEFAULT_SERVER_PROPERTIES_CONF_FILE="conf/props/server.txt";

	public static final String DEFAULT_CLIENT_COMM_STACK = "pt.fct.di.clientProxy.comm.SimpleTCPComm";
	public static final String DEFAULT_CLIENT_PROPERTIES_CONF_FILE="conf/props/client.txt";
	
	public static final String DEFAULT_HBASE_CONF_FILE = "conf/comm/dbs/hbase/hbase-site.xml";
//	public static final String DEFAULT_HBASE_CONF_FILE_2 = "conf/comm/dbs/hbase/hbase-default.xml";
	
	public static final String DEFAULT_MULTICAST_IP = "224.0.0.3";
	public static final String DEFAULT_MULTICAST_PORT = "6789";
	
	public static final String DEFAULT_UNICAST_PORT="4445";
	
	public static final String DEFAULT_TCP_IP = "localhost";
	public static final String DEFAULT_TCP_PORT = "1500";
	
	public static final String DEFAULT_MIN_POOL_THREADS = "2";
	public static final String DEFAULT_MAX_POOL_THREADS = "3";
	
	public static final String DEFAULT_THREAD_TIME_TO_LIVE = "1000";
	
	public static final String DEFAULT_MESSAGE_BUFFER_SIZE = "20";
	
	public static final String DEFAULT_OPERATION_TIMEOUT = "100";
	public static final String DEFAULT_OPERATION_THRESHOLD = "100";
	
	public static final String DEFAULT_RCP_TIMEOUT="20000";

}
