package com.yahoo.ycsb.db;

import static junit.framework.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <p>{@link MembaseClient} unit tests.</p>
 * 
 * <p>Copyright &copy; 2010 Videosurf.</p>
 * @author <a href="echen@videoegg.com">Eric Chen</a>
 */
public class MembaseClientTest {

	private MembaseClient membaseClient;
	
	@Before
	public void setUp() throws Exception {
		membaseClient = new MembaseClient();
		membaseClient.setProperties(new Properties() {{
//			put(MembaseClient.Config.Key.DEBUG, "true");
			put(MembaseClient.Config.Key.RECORD_EXPIRATION_SECONDS, "1");
			put(MembaseClient.Config.Key.HOSTS, "10.10.1.188:11211");
		}});
		membaseClient.init();		
	}

	@After
	public void tearDown() throws Exception {
		if (membaseClient != null) {
			membaseClient.cleanup();
		}
	}

	/**
	 * Tests inserting and reading records.
	 */
	@Test
	public void testInsertRead() {
		final String TABLE1 = "testInsertRead";
		final String NON_EXISTENT_TABLE = "non-existent-table";
		final String RECORD1 = "record1";
		final String NON_EXISTENT_RECORD = "non-existent-record";
		final String FIELD1 = "field1";
		final String FIELD2 = "field2";
		final String VALUE1 = "value1";
		final String VALUE2 = "value2";
		final HashMap<String,String> record1 = new HashMap<String,String>();
		record1.put(FIELD1, VALUE1);
		record1.put(FIELD2, VALUE2);
		membaseClient.insert(TABLE1, RECORD1, record1);

		// read from non-existent table
		final HashMap<String,String> nonExistentTableFields = new HashMap<String,String>();
		membaseClient.read(NON_EXISTENT_TABLE, RECORD1, null, nonExistentTableFields);
		assertEquals("Unexpected fields in read from non-existent table", 0, nonExistentTableFields.size());

		// read non-existent record
		final HashMap<String,String> nonExistentRecordFields = new HashMap<String,String>();
		membaseClient.read(TABLE1, NON_EXISTENT_RECORD, null, nonExistentRecordFields);
		assertEquals("Unexpected fields in read from non-existent record", 0, nonExistentRecordFields.size());
		
		// read all fields
		final HashMap<String,String> record1AllFields = new HashMap<String,String>();
		membaseClient.read(TABLE1, RECORD1, null, record1AllFields);
		assertEquals("Unexpected number of fields in record 1 all fields", 2, record1AllFields.size());
		assertEquals("Unexpected value for field 1 of record 1 all fields", VALUE1, record1AllFields.get(FIELD1));
		assertEquals("Unexpected value for field 2 of record 1 all fields", VALUE2, record1AllFields.get(FIELD2));

		// read specific fields
		final HashMap<String,String> record1SelectFields = new HashMap<String,String>();
		final Set<String> record1Fields = new HashSet<String>();
		record1Fields.add(FIELD2);
		membaseClient.read(TABLE1, RECORD1, record1Fields, record1SelectFields);
		assertEquals("Unexpected number of fields in record 1 select fields", 1, record1SelectFields.size());
		assertEquals("Unexpected value for field 2 of record 1 all fields", VALUE2, record1SelectFields.get(FIELD2));
	}
}