package com.hazelcast.impl;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class MapEntryListenerTest {

	final String n = "foo";
	
	HazelcastInstance h1 ; 
	HazelcastInstance h2 ;
	IMap<String, String> map1;
	IMap<String, String> map2;
	
	AtomicInteger globalCount = new AtomicInteger();
	AtomicInteger localCount = new AtomicInteger();
	
	@BeforeClass
	@AfterClass
	public static void cleanup() throws Exception {
		Hazelcast.shutdownAll();
	}
	
	@Before
	public void before() {
		globalCount.set(0);
		localCount.set(0);
		
		h1 = Hazelcast.newHazelcastInstance(null); 
		map1 = h1.getMap(n);
		h2 = Hazelcast.newHazelcastInstance(null);
		map2 = h2.getMap(n);
	}
	
	@After
	public void after() {
		h1.getLifecycleService().shutdown();
		h2.getLifecycleService().shutdown();
	}

	@Test
	public void globalListenerTest() throws InterruptedException {
		map1.addEntryListener(createEntryListener(false), true);
		map2.addEntryListener(createEntryListener(false), true);

		int k = 3;
		putDummyData(k);
		checkCountWithExpected(k * 2, 0);
	}

	@Test
	public void localListenerTest() throws InterruptedException {
		map1.addLocalEntryListener(createEntryListener(true));
		map2.addLocalEntryListener(createEntryListener(true));

		int k = 4;
		putDummyData(k);
		checkCountWithExpected(0, k);
	}

	@Test
	/**
	 * Test for issue 584
	 */
	public void globalAndLocalListenerTest() throws InterruptedException {
		map1.addLocalEntryListener(createEntryListener(true));
		map2.addLocalEntryListener(createEntryListener(true));
		map1.addEntryListener(createEntryListener(false), true);
		map2.addEntryListener(createEntryListener(false), true);
		
		int k = 1;
		putDummyData(k);
		checkCountWithExpected(k * 2, k);
	}
	
	@Test
	/**
	 * Test for issue 584
	 */
	public void globalAndLocalListenerTest2() throws InterruptedException {
		// changed listener order
		map1.addEntryListener(createEntryListener(false), true);
		map1.addLocalEntryListener(createEntryListener(true));
		map2.addEntryListener(createEntryListener(false), true);
		map2.addLocalEntryListener(createEntryListener(true));

		int k = 3;
		putDummyData(k);
		checkCountWithExpected(k * 2, k);
	}
	
	private void putDummyData(int k) {
		for (int i = 0; i < k; i++) {
			map1.put("foo" + i, "bar");
		}
	}
	
	private void checkCountWithExpected(int expectedGlobal, int expectedLocal) throws InterruptedException {
		// wait for entry listener execution
		Thread.sleep(1000 * 3);
		Assert.assertEquals(expectedLocal, localCount.get());
		Assert.assertEquals(expectedGlobal, globalCount.get());
	}

	private EntryListener<String, String> createEntryListener(final boolean isLocal) {
		return new EntryListener<String, String>() {
			private final boolean local = isLocal;
			public void entryUpdated(EntryEvent<String, String> event) {
			}

			public void entryRemoved(EntryEvent<String, String> event) {
			}

			public void entryEvicted(EntryEvent<String, String> event) {
			}

			public void entryAdded(EntryEvent<String, String> event) {
				if(local) {
					localCount.incrementAndGet();
					System.out.println("LOCAL-EVENT => " + event);
				}
				else {
					globalCount.incrementAndGet();
					System.out.println("GLOBAL-EVENT => " + event);
				}
			}
		};
	}
}
