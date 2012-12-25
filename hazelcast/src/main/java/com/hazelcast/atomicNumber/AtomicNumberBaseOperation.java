package com.hazelcast.atomicNumber;

import com.hazelcast.atomicNumber.proxy.AtomicNumberService;
import com.hazelcast.spi.KeyBasedOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

// author: sancar - 24.12.2012
public abstract class AtomicNumberBaseOperation extends AbstractNamedOperation{

    public AtomicNumberBaseOperation(){
        super();
    }

    public AtomicNumberBaseOperation(String name){
        super(name);
    }

    public long getNumber(){
        return  AtomicNumberService.getNumber(name);
    }

    public void setNumber(long value){
        AtomicNumberService.setNumber(name, value);
    }

}
