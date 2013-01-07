package com.hazelcast.atomicnumber;

import com.hazelcast.spi.impl.AbstractNamedOperation;

// author: sancar - 24.12.2012
public abstract class AtomicNumberBaseOperation extends AbstractNamedOperation {

    public AtomicNumberBaseOperation() {
        super();
    }

    public AtomicNumberBaseOperation(String name) {
        super(name);
    }

    public long getNumber() {
        return ((AtomicNumberService) getService()).getNumber(name);
    }

    public void setNumber(long value) {
        ((AtomicNumberService) getService()).setNumber(name, value);
    }

}
