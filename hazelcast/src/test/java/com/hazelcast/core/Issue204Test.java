package com.hazelcast.core;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import static org.junit.Assert.*;


/**
 * Test for issue #204:
 * http://code.google.com/p/hazelcast/issues/detail?id=204
 * 
 * Summary:
 * 
 * 1. If you run one instance, passing it 'true' (thus filling the Map) it
 * works fine - all 50 entries are added and, after one minute, evicted
 * (entryEvicted() is called).
 * 2. If you run two instances, the first one 'false' and the second one
 * 'true', it fills the map and evicts a small portion (around 10) of the
 * entries from both instances after one minute. Actually, that's not true. In
 * fact *all* 50 entries are evicted but the entryEvicted() method is only
 * called around 10 times.
 * 3. If you run two instances, passing both of them 'true', it fills the map
 * on the first instance, then updates all entries when the second instance
 * starts, and all 50 entries are evicted after one minute.
 */
public class Issue204Test {
	
	public static final int TIMEOUT_SECONDS = 5;
	public static final int ENTRIES = 20;

	private void sleep(int seconds) {
		System.out.println("Sleeping "+seconds+" seconds.");
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void testOneMember(CountingMember member) {
		member.fill();
		
		sleep(2 * TIMEOUT_SECONDS);
		Hazelcast.shutdownAll();
		
		assertEquals(ENTRIES, member.added);
		assertEquals(0, member.updated);
		assertEquals(ENTRIES, member.evicted);
	}
	
	@Test
	public void testOneMemberInteger() {
		testOneMember(new IntegerMapMember());
	}
	
	@Test
	public void testOneMemberString() {
		testOneMember(new StringMapMember());
	}
	
	
	private void testTwoMembersOnlyOneFilling(CountingMember member1, CountingMember member2) {
		member2.fill();
		
		sleep(4 * TIMEOUT_SECONDS);
		Hazelcast.shutdownAll();
		
		assertEquals(ENTRIES, member1.added);
		assertEquals(0, member1.updated);
		assertEquals(ENTRIES, member1.evicted);
		
		assertEquals(ENTRIES, member2.added);
		assertEquals(0, member2.updated);
		assertEquals(ENTRIES, member2.evicted);
	}
	
	@Test
	public void testTwoMembersOnlyOneFillingInteger() {
		testTwoMembersOnlyOneFilling(new IntegerMapMember(), new IntegerMapMember());
	}
	
	@Test
	public void testTwoMembersOnlyOneFillingString() {
		testTwoMembersOnlyOneFilling(new StringMapMember(), new StringMapMember());
	}

	
	private void testTwoMembersBothFilling(CountingMember member1, CountingMember member2) {
		member1.fill();
		member2.fill();
		
		sleep(4 * TIMEOUT_SECONDS);
		Hazelcast.shutdownAll();
		
		assertEquals(ENTRIES, member1.added);
		assertEquals(ENTRIES, member1.updated);
		assertEquals(ENTRIES, member1.evicted);
		
		assertEquals(ENTRIES, member2.added);
		assertEquals(ENTRIES, member2.updated);
		assertEquals(ENTRIES, member2.evicted);
	}
	
	@Test
	public void testTwoMembersBothFillingInteger() {
		testTwoMembersBothFilling(new IntegerMapMember(), new IntegerMapMember());
	}
	
	@Test
	public void testTwoMembersBothFillingString() {
		testTwoMembersBothFilling(new StringMapMember(), new StringMapMember());
	}

	
	public abstract class CountingMember {

		protected int added = 0;
		protected int updated = 0;
		protected int evicted = 0;

		public abstract void fill();

	}
	
	public abstract class AbstractMember<K, V> extends CountingMember implements EntryListener<K, V> {
		protected final static String CLUSTER_NAME = "issue204cluster";
		protected HazelcastInstance hz;
		protected IMap<K, V> map;

		public AbstractMember() {
			Config cfg = new XmlConfigBuilder().build().setGroupConfig(new GroupConfig(CLUSTER_NAME));
			this.hz = Hazelcast.newHazelcastInstance(cfg);
			this.map = this.hz.getMap("issue204map");
			map.addEntryListener(this,true);
		}
		
		public void entryAdded(EntryEvent<K, V> event) {
			log(event);
			added++;
		}
		
		public void entryRemoved(EntryEvent<K, V> event) {
			log(event);
		}
		
		public void entryUpdated(EntryEvent<K, V> event) {
			log(event);
			updated++;
		}       
		
		public void entryEvicted(EntryEvent<K, V> event) {
			log(event);
			evicted++;
		}
		
		private void log(EntryEvent<K, V> event) {
			Calendar cal = Calendar.getInstance();
			System.out.println("Event "+event.getEventType().toString()+" at "+cal.getTime()+" for key "+event.getKey()+", value "+event.getValue());
		}
	}
	
	public class IntegerMapMember extends AbstractMember<Integer, Integer>
	{
		public void fill() {
			for (int i = 0; i < ENTRIES; i++) {
				map.put(Integer.valueOf(i), Integer.valueOf(i),TIMEOUT_SECONDS,TimeUnit.SECONDS);
			}
		}
	}
	
	public class StringMapMember extends AbstractMember<String, String>
	{
		public void fill() {
			for (int i = 0; i < ENTRIES; i++) {
				map.put(String.valueOf(i), String.valueOf(i),TIMEOUT_SECONDS,TimeUnit.SECONDS);
			}
		}
	}
}


