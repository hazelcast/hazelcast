package com.hazelcast.client;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;

import com.hazelcast.core.ItemListener;

public class CountDownItemListener<E> implements ItemListener<E>{
	final CountDownLatch itemAddLatch;
    final CountDownLatch itemRemovedLatch;
    
	public CountDownItemListener(CountDownLatch itemAddLatch, CountDownLatch itemRemovedLatch) {
		this.itemAddLatch = itemAddLatch;
		this.itemRemovedLatch = itemRemovedLatch;
	}
	public void itemAdded(E item) {
		System.out.println(item);
		itemAddLatch.countDown();
	
	}

	public void itemRemoved(E item) {
		itemRemovedLatch.countDown();
		
	}
	
}