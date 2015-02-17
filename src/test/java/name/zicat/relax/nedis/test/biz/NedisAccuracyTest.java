package name.zicat.relax.nedis.test.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import name.zicat.relax.nedis.biz.Nedis;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Nedis data accuracy test
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-26
 */
public class NedisAccuracyTest {
	private Nedis nedis = null;
	
	@Before
	public void init() throws Exception{
		nedis = new Nedis();
		nedis.getResource();
	}
	
	@After
	public void des() throws Exception{
		Nedis.destroyPool();
	}
	/**
	 * test String read and write
	 */
	@Test
	public void testString (){
		/** add one record **/
		System.out.println(nedis.addString(1, "test1", "value_test1"));
		System.out.println(nedis.getString(1, "test1"));
		
		/** add record batch **/
		Map<String, String> keyValues = new HashMap<String, String>();
		keyValues.put("test2", "value_test2");
		keyValues.put("test3", "value_test3");
		keyValues.put("test4", "value_test4");
		System.out.println(nedis.addStringBach(1, keyValues));
		List<String>keys = new ArrayList<String>();
		keys.add("test1");
		keys.add("test2");
		keys.add("test3");
		keys.add("test4");
		printStringBatch(nedis.getStringBatch(1, keys));
		Nedis.destroyPool();
	}
	
	public void printStringBatch(Map<String, String>value) {
		for(Entry<String, String> entry:value.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}
	
	/**
	 * test List read and write
	 */
	@Test
	public void testList() {
		List<String> key1Values = new ArrayList<String>();
		key1Values.add("value1_key1");
		key1Values.add("value2_key1");
		key1Values.add("value3_key1");
		
		nedis.addList2Nedis(2, "key1", key1Values, false);
		
		List<String> key2Values = new ArrayList<String>();
		key2Values.add("value1_key2");
		key2Values.add("value2_key2");
		key2Values.add("value3_key2");
		
		List<String> key3Values = new ArrayList<String>();
		key3Values.add("value1_key3");
		key3Values.add("value2_key3");
		key3Values.add("value3_key3");
		
		Map<String, List<String>> batchList = new HashMap<String,List<String>>();
		batchList.put("key2", key2Values);
		batchList.put("key3", key3Values);
		
		nedis.addList2NedisBatch(2, batchList, false);
		
		print(nedis.getList(2, "key1"));
		System.out.println("========================");
		print(nedis.getListRange(2, "key2", 0, 2));
		System.out.println("========================");
		
		List<String> keys = new ArrayList<String>();
		keys.add("key1");
		keys.add("key2");
		keys.add("key3");
		print(nedis.getListBatch(2, keys));
		Nedis.destroyPool();
		
	}
	
	/**
	 * test Set read and write
	 */
	@Test
	public void testSet() {
		Set<String> key1Values = new HashSet<String>();
		key1Values.add("value1_key1");
		key1Values.add("value2_key1");
		key1Values.add("value3_key1");
		
		nedis.addSet2Nedis(3, "key1", key1Values, false);
		
		Set<String> key2Values = new HashSet<String>();
		key2Values.add("value1_key2");
		key2Values.add("value2_key2");
		key2Values.add("value3_key2");
		
		Set<String> key3Values = new HashSet<String>();
		key3Values.add("value1_key3");
		key3Values.add("value2_key3");
		key3Values.add("value3_key3");
		
		Map<String, Set<String>> batchList = new HashMap<String,Set<String>>();
		batchList.put("key2", key2Values);
		batchList.put("key3", key3Values);
		
		nedis.addSet2NedisBatch(3, batchList, false);
		
		print(nedis.getSet(3, "key1"));
		System.out.println("========================");
	
		List<String> keys = new ArrayList<String>();
		keys.add("key1");
		keys.add("key2");
		keys.add("key3");
		print(nedis.getSetBatch(3, keys),true);
	}
	
	/**
	 * test map read and write
	 */
	@Test
	public void testMap(){
		
		nedis.addHashMap2Nedis(4, "key1", "field1", "value1", true);
		nedis.addHashMap2Nedis(4, "key1", "field2", "value2", false);
		
		Map<String, Map<String, String>> mapBatch = new HashMap<String, Map<String, String>>();
		Map<String, String> value2 = new HashMap<String, String>();
		value2.put("field1", "value1");
		value2.put("field2", "value2");
		value2.put("field3", "value3");
		mapBatch.put("key2", value2);
		Map<String, String> value3 = new HashMap<String, String>();
		value3.put("field1", "value1");
		value3.put("field2", "value2");
		value3.put("field3", "value3");
		mapBatch.put("key3", value3);
		nedis.addHashMap2NedisBatch(4, mapBatch, true);
		
		printMap(nedis.getHashMap(4, "key1"));
		System.out.println("=====================");
		
		List<String> keys = new ArrayList<String>();
		keys.add("key1");
		keys.add("key2");
		keys.add("key3");
		printMapMap(nedis.getHashMapBatch(4, keys));
		System.out.println("=============================");
		System.out.println(nedis.getHashMapVal(4, "key1", "field1"));
	}
	
	/**
	 * test contains function
	 */
	@Test
	public void testContains(){
		System.out.println(nedis.contains(4, "key1"));
		System.out.println(nedis.contains(2, "key8"));
	}
	
	/**
	 * test randomKey function
	 */
	@Test
	public void testRandomKey() {
		System.out.println(nedis.randomKey(1));
		System.out.println(nedis.randomKey(2));
		System.out.println(nedis.randomKey(3));
	}
	
	/**
	 * test remove function
	 */
	@Test
	public void testRemove() {
		System.out.println(nedis.remove(1, "test3"));
		System.out.println(nedis.remove(2, "key3"));
		System.out.println(nedis.remove(3, "key2"));
		System.out.println(nedis.remove(3, "key111"));
	}
	
	private void print(List<String> values) {
		for(String str: values){
			System.out.println(str);
		}
	}
	
	private void print(Set<String> values) {
		for(String str: values){
			System.out.println(str);
		}
	}
	
	private void print(Map<String, List<String>> values) {
		for(Entry<String, List<String>>entry : values.entrySet()) {
			System.out.println("---------------");
			System.out.println(entry.getKey());
			print(entry.getValue());
		}
	}
	
	private void print(Map<String, Set<String>> values,boolean f) {
		for(Entry<String, Set<String>>entry : values.entrySet()) {
			System.out.println("---------------");
			System.out.println(entry.getKey());
			print(entry.getValue());
		}
	}
	
	private void printMap(Map<String, String> values) {
		for(Entry<String, String>entry: values.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
		}
	}
	
	private void printMapMap(Map<String, Map<String, String>>values){
		for(Entry<String, Map<String, String>>entry : values.entrySet()) {
			System.out.println("========================");
			System.out.println(entry.getKey());
			for(Entry<String, String>childEntry : entry.getValue().entrySet()) {
				System.out.println("            "+childEntry.getKey());
				System.out.println("            "+childEntry.getValue());
			}
		}
	}
}
