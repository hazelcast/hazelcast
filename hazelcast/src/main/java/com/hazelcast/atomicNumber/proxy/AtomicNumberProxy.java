package com.hazelcast.atomicNumber.proxy;

import com.hazelcast.atomicNumber.*;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.LocalAtomicNumberStats;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ServiceProxy;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

// author: sancar - 21.12.2012
public class AtomicNumberProxy implements ServiceProxy, AtomicNumber {

    private String name;
    private NodeEngine nodeEngine;
    private final int partitionId;

    public AtomicNumberProxy(String name, NodeEngine nodeEngine) {

        this.name = name;
        this.nodeEngine = nodeEngine;
        this.partitionId = nodeEngine.getPartitionId(nodeEngine.toData(name));
    }

    public String getName() {
        return name;
    }

    public long addAndGet(long delta) {

        try {
            AddAndGetOperation operation = new AddAndGetOperation(name, delta);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long)f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public boolean compareAndSet(long expect, long update) {
        try {
            CompareAndSetOperation operation = new CompareAndSetOperation(name, expect, update);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Boolean)f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public long decrementAndGet() {
        return addAndGet(-1);
    }

    public long incrementAndGet() {
        return addAndGet(1);
    }

    public long get() {
        return addAndGet(0);
    }

    public long getAndAdd(long delta) {
        try {
            GetAndAddOperation operation = new GetAndAddOperation(name, delta);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long)f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public long getAndSet(long newValue) {
        try {
            GetAndSetOperation operation = new GetAndSetOperation(name, newValue);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long)f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public void set(long newValue) {
        try {
            SetOperation operation = new SetOperation(name, newValue);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            inv.invoke();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    @Deprecated
    public boolean weakCompareAndSet(long expect, long update) {
        return false;
    }

    @Deprecated
    public void lazySet(long newValue) {

    }

    public LocalAtomicNumberStats getLocalAtomicNumberStats() {
        return null;
    }

    public InstanceType getInstanceType() {
        return InstanceType.ATOMIC_NUMBER;
    }

    public void destroy() {

    }

    public Object getId() {
        return name;
    }

    public static void main(String [] args) throws Exception {
        Config cfg = new XmlConfigBuilder("/Users/msk/IdeaProjects/sample/src/main/resources/hazelcast.xml").build();
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
        final AtomicNumber a1 = hazelcastInstance.getAtomicNumber("one");

        final AtomicNumber a2 = hazelcastInstance.getAtomicNumber("two");

        final boolean x;

        for(int i = 0 ; i < 1000 ; i++){
            Object returned = new Callable() {

                public Object call() throws Exception {

                    for(int j = 0 ; j < 1; j++)
                        a1.incrementAndGet();

                    if(a1.compareAndSet(1000,-1)){
                        a1.set(2);
                        return true;
                    }


                    return false;
                }
            }.call();

        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("a1 : " + a1.get());

//        System.out.println("a2 : " + a2.get());
    }
}
