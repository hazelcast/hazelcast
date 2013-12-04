package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

public class AlterOperation extends AbstractAlterOperation {

    public AlterOperation() {
    }

    public AlterOperation(String name, Data function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Function<Long,Long> f = nodeEngine.toObject(function);
        AtomicLongWrapper reference = getNumber();

        long input = reference.get();
        long output = f.apply(input);
        shouldBackup = input!=output;
        if(shouldBackup){
            backup = output;
            reference.set(backup);
        }
    }
}

