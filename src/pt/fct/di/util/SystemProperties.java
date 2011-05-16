package pt.fct.di.util;

import java.util.Properties;

public class SystemProperties {
	
//	private static int _clientId;
//	
//	private static String _tableName;
//	private static String _columnFamily;
//	
//	private static int _minPoolThreads;
//	private static int _maxPoolThreads;
//	private static int _messageBufferSize;
//	
//	private static long _timeToLive;
	private static long _rpcTimeoutInMilis;
//	private static long _operationTimeout;
//	private static long _operationThreshold;
//	
	public static boolean _debug;
	
	public static Properties _properties;
	
	public static void init(Properties p)
	{
		_properties = p;
		
//		String id = p.getProperty("clientId");
//		if(id == null) 
//		{
//			System.err.println("Property \"Client Id\" missing");
//			System.exit(-1);
//		}
//		_clientId = Integer.parseInt(id);
		
//		_tableName = p.getProperty("tableName", Constants.DEFAULT_DB_TABLE_NAME);
//		_columnFamily = p.getProperty("columnFamily", Constants.DEFAULT_DB_FAMILY_COLUMN);
		
//		_minPoolThreads = Integer.parseInt(p.getProperty("minPoolThreads", Constants.DEFAULT_MIN_POOL_THREADS));
//		_maxPoolThreads = Integer.parseInt(p.getProperty("maxPoolThreads", Constants.DEFAULT_MAX_POOL_THREADS));
//		_messageBufferSize = Integer.parseInt(p.getProperty("messageBufferSize", Constants.DEFAULT_MESSAGE_BUFFER_SIZE));
		
//		_timeToLive = Long.parseLong(p.getProperty("threadTimeToLive", Constants.DEFAULT_THREAD_TIME_TO_LIVE));
		_rpcTimeoutInMilis = Long.parseLong(p.getProperty("rpcTimeOut", Constants.DEFAULT_RCP_TIMEOUT));
//		_operationTimeout = Long.parseLong(p.getProperty("operationTimeout", Constants.DEFAULT_OPERATION_TIMEOUT));
//		_operationThreshold = Long.parseLong(p.getProperty("operationThreshold", Constants.DEFAULT_OPERATION_THRESHOLD));
//		
		_debug = Boolean.parseBoolean(p.getProperty("debug", "false"));
	}
	
//	public static int getClientId()
//	{
//		return _clientId;
//	}
//
//	public static String getTableName() {
//		return _tableName;
//	}
//
//	public static String getColumnFamily() {
//		return _columnFamily;
//	}
//
//	public static int getMinPoolThreads() {
//		return _minPoolThreads;
//	}
//
//	public static int getMaxPoolThreads() {
//		return _maxPoolThreads;
//	}
//
//	public static int getMessageBufferSize() {
//		return _messageBufferSize;
//	}
//
//	public static long getTimeToLive() {
//		return _timeToLive;
//	}
//
	public static long getRpcTimeout() {
		return _rpcTimeoutInMilis;
	}
//
//	public static long getOperationTimeout() {
//		return _operationTimeout;
//	}
//
//	public static long getOperationThreshold() {
//		return _operationThreshold;
//	}
	
	public static boolean isSetProperty(String prop)
	{
		return _properties.containsKey(prop);
	}
	
	public static String getPropertie(String prop)
	{
		return _properties.getProperty(prop);
	}
	
	public static String testAndGetPropertie(String prop) throws Exception
	{
		if(!isSetProperty(prop)) throw new Exception("Propertie "+prop+" must be set in config file");
		return _properties.getProperty(prop);
	}
	
	public static Properties getProperties()
	{
		return _properties;
	}
}
