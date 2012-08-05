package com.hazelcast.jca;

import java.util.concurrent.ExecutorService;

import javax.resource.cci.Connection;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;

/** Subset of HazelcastInstance */
public interface HazelcastConnection extends Connection {

    /**
     * @see HazelcastInstance#getQueue(String)
     */
    <E> IQueue<E> getQueue(String name);

    /**
     * @see HazelcastInstance#getTopic(String)
     */
    <E> ITopic<E> getTopic(String name);

    /**
     * @see HazelcastInstance#getSet(String)
     */
    <E> ISet<E> getSet(String name);

    /**
     * @see HazelcastInstance#getList(String)
     */
    <E> IList<E> getList(String name);

    /**
     * @see HazelcastInstance#getMap(String)
     */
    <K, V> IMap<K, V> getMap(String name);

    /**
     * @see HazelcastInstance#getMultiMap(String)
     */
    <K, V> MultiMap<K, V> getMultiMap(String name);
    
    /**
     * @see HazelcastInstance#getExecutorService
     */
    ExecutorService getExecutorService();

    /**
     * @see HazelcastInstance#getExecutorService(String)
     */
    ExecutorService getExecutorService(String name);

    /**
     * @see HazelcastInstance#getAtomicNumber(String)
     */
    AtomicNumber getAtomicNumber(String name);

    /**
     * @see HazelcastInstance#getCountDownLatch(String)
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * @see HazelcastInstance#getSemaphore(String)
     */
    ISemaphore getSemaphore(String name);
}
