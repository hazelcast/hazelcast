package com.hazelcast.atomicNumber;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.monitor.LocalAtomicNumberStats;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ServiceProxy;

// author: sancar - 21.12.2012
public class AtomicNumberProxy implements ServiceProxy , AtomicNumber {

    private String name;
    private NodeEngine nodeEngine;

    public AtomicNumberProxy(String name, NodeEngine nodeEngine) {

        this.name = name;
        this.nodeEngine = nodeEngine;
    }

    public String getName() {
        return name;
    }

    public long addAndGet(long delta) {
        return 0;
    }

    public boolean compareAndSet(long expect, long update) {
        return false;
    }

    public long decrementAndGet() {
        return 0;
    }

    public long get() {
        return 0;
    }

    public long getAndAdd(long delta) {
        return 0;
    }

    public long getAndSet(long newValue) {
        return 0;
    }

    public long incrementAndGet() {
        return 0;
    }

    public void set(long newValue) {

    }

    public boolean weakCompareAndSet(long expect, long update) {
        return false;
    }

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
}
