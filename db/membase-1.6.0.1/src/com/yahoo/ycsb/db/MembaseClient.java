package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * <p>Membase implementation of DB interface.</p>
 * 
 * <p>Copyright &copy; 2010 Videosurf.</p>
 * @author <a href="echen@videoegg.com">Eric Chen</a>
 */
public class MembaseClient extends DB
{
	/**
	 * <p>Startup configuration.</p>
	 * 
	 * <p>Copyright &copy; 2010 Videosurf.</p>
	 * @author <a href="echen@videoegg.com">Eric Chen</a>
	 */
	public static class Config {
		/**
		 * <p>Startup configuration keys.</p>
		 * 
		 * <p>Copyright &copy; 2010 Videosurf.</p>
		 * @author <a href="echen@videoegg.com">Eric Chen</a>
		 */
		public static class Key {
			public static final String DEBUG = "debug";
			public static final String HOSTS = "hosts";
			public static final String OPERATION_TIMEOUT_MILLIS = "membase.operation.timeout.millis";
			public static final String RECORD_EXPIRATION_SECONDS = "membase.record.expiration.seconds";
		}

		/**
		 * <p>Startup configuration default values.</p>
		 * 
		 * <p>Copyright &copy; 2010 Videosurf.</p>
		 * @author <a href="echen@videoegg.com">Eric Chen</a>
		 */
		public static class Default {
			public static final String OPERATION_TIMEOUT_MILLIS = String.valueOf(DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT); // 1000 ms
			public static final String RECORD_EXPIRATION_SECONDS = "0"; // never expire
		}
	}

	/**
	 * <p>{@link DB} interface return codes.</p>
	 * 
	 * <p>Copyright &copy; 2010 Videosurf.</p>
	 * @author <a href="echen@videoegg.com">Eric Chen</a>
	 */
	public static class ReturnCode {
		public static final int OK = 0;
		public static final int ERROR = -1;
	}
	
	private static final String TABLE_RECORD_SEPARATOR = ":";

	// configuration
	private boolean isDebug;
	private long operationTimeoutMillis;
	private int recordExpirationSeconds;

	private MemcachedClient membaseClient;
	
	@Override
	public void init() throws DBException {
		final Properties properties = getProperties();
		isDebug = Boolean.parseBoolean(properties.getProperty(Config.Key.DEBUG));
		operationTimeoutMillis = Long.parseLong(properties.getProperty(Config.Key.OPERATION_TIMEOUT_MILLIS, Config.Default.OPERATION_TIMEOUT_MILLIS));
		recordExpirationSeconds = Integer.parseInt(properties.getProperty(Config.Key.RECORD_EXPIRATION_SECONDS, Config.Default.RECORD_EXPIRATION_SECONDS));

		// init single membase client
		final String hosts = properties.getProperty(Config.Key.HOSTS);
		if (hosts == null)
		{
			throw new DBException("Required property \"" + Config.Key.HOSTS + "\" missing for " + MembaseClient.class.getSimpleName());
		}
		try {
			membaseClient = 
				new MemcachedClient(
					new ConnectionFactoryBuilder()
						.setOpTimeout(operationTimeoutMillis)
						.build(),
					AddrUtil.getAddresses(hosts));		
		}
		catch (IOException exception)
		{
			throw new DBException(exception);
		}
	}

	@Override
	public void cleanup()
	{
		if (membaseClient != null) {
			membaseClient.shutdown();
		}
	}
	
	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String,String> result)
	{
		final Map<String,String> record = 
			(Map<String,String>)membaseClient.get(
				getTableRecordKey(table, key));
		if (record != null) {
			if (fields == null)
			{
				result.putAll(record);
			}
			else
			{
				for (final Map.Entry<String,String> field : record.entrySet())
				{
					final String fieldName = field.getKey();
					if (fields.contains(fieldName))
					{
						result.put(fieldName, field.getValue());
					}
				}
			}
		}

		// debug output
		if (isDebug)
		{
			final StringBuffer debugBuffer = new StringBuffer();
			debugBuffer.append("READ: ");
			appendRecord(debugBuffer, result);
			System.out.println(debugBuffer.toString());
		}
		return ReturnCode.OK;
	}

	@Override
	public int readMultiple(String table, Set<String> keys, Set<String> fields, List<Map<String,String>> result)
	{
		// map table and record keys to flattened table-record space
		final Set<String> tableKeys = new HashSet<String>();
		for (final String key : keys)
		{
			tableKeys.add(getTableRecordKey(table, key));
		}
		
		// debug output
		if (isDebug) {
			System.out.println("READ MULTIPLE: ");
		}
		
		final Map<String,Object> records = membaseClient.getBulk(tableKeys);
		for (final Map.Entry<String,Object> record : records.entrySet())
		{
			final Map<String,String> recordFields = (Map<String,String>)record.getValue();
			Map<String,String> resultRecord;
			if (fields == null)
			{
				resultRecord = recordFields;
			}
			else
			{
				resultRecord = new HashMap<String,String>();
				for (final Map.Entry<String,String> recordField : recordFields.entrySet())
				{
					final String fieldName = recordField.getKey();
					if (fields.contains(fieldName))
					{
						resultRecord.put(fieldName, recordField.getValue());
					}
				}
			}
			result.add(resultRecord);

			// debug output
			if (isDebug) {
				final StringBuffer debugBuffer = new StringBuffer();
				appendRecord(debugBuffer, resultRecord);
				System.out.println(debugBuffer.toString());
			}
		}
		return ReturnCode.OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, String>> result)
	{
		throw new UnsupportedOperationException("Scanning not supported by membase");
	}

	@Override
	public int insert(String table, String key, HashMap<String,String> values) {
		return update(table, key, values);
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values)
	{
		// debug output
		if (isDebug)
		{
			final StringBuffer debugBuffer = new StringBuffer();
			debugBuffer.append("INSERT ");
			appendRecord(debugBuffer, values);
			System.out.println(debugBuffer.toString());
		}

		return handleTaskExecution(
			membaseClient.set(
				getTableRecordKey(table, key), 
				recordExpirationSeconds, 
				values));
	}

	@Override
	public int delete(String table, String key)
	{
		return handleTaskExecution(
			membaseClient.delete(
				getTableRecordKey(table, key)));
	}

	/**
	 * Returns a table-record key combination.
	 * 
	 * @param table the table name.
	 * @param record the record key.
	 * @return a table-record key combination.
	 */
	protected String getTableRecordKey(String table, String record)
	{
		return table + TABLE_RECORD_SEPARATOR + record;
	}

	/**
	 * Returns the record key from a table-record key combination.
	 * 
	 * @param tableRecord a table-record key combination.
	 * @return the record key.
	 */
	protected String getRecordKey(String tableRecord)
	{
		return tableRecord.split(TABLE_RECORD_SEPARATOR)[1];
	}

	/**
	 * Appends a string representation of the input <code>record</code> to the 
	 * input <code>stringBuffer</code>. 
	 * 
	 * @param stringBuffer the string buffer to append the record to.
	 * @param record the record to append to the string buffer.
	 */
	protected void appendRecord(StringBuffer stringBuffer, Map<String,String> record)
	{
		for (final Map.Entry<String,String> field : record.entrySet())
		{
			stringBuffer.append("(");
			stringBuffer.append(field.getKey());
			stringBuffer.append("=");
			stringBuffer.append(field.getValue());
			stringBuffer.append(")");
		}
	}

	/**
	 * Handles the execution results of the input <code>Future</code> task.
	 * 
	 * @param future the <code>Future</code> task whose execution results 
	 * to process.
	 * @return a success/error return code.
	 */
	protected int handleTaskExecution(Future<?> future)
	{
		int returnCode;
		try
		{
			awaitTaskCompletion(future);
			returnCode = ReturnCode.OK;
		}
		catch (ExecutionException exception)
		{
			returnCode = ReturnCode.ERROR;
			exception.printStackTrace();
			exception.printStackTrace(System.out);
		}
		return returnCode;
	}
	
	/**
	 * Awaits the completion of a <code>Future</code> task, effectively making an 
	 * asynchronous process synchronous.
	 * 
	 * @param <T> the <code>Future</code> return type.
	 * @param future the <code>Future</code> task to await completion of.
	 * @return the <code>Future</code> return value.
	 * @throws ExecutionException if an error occurs during <code>Future</code> execution.
	 */
	protected <T> T awaitTaskCompletion(Future<T> future) throws ExecutionException
	{
		do
		{
			try
			{
				// wait for insert to complete
				return future.get();
			}
			catch (InterruptedException exception)
			{
				// continue waiting if not done
			}
		}
 		while (true);
	}
}