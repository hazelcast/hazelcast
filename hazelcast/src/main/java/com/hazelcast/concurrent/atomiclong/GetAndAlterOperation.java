package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

public class GetAndAlterOperation extends AbstractAlterOperation {

    public GetAndAlterOperation() {
    }

    public GetAndAlterOperation(String name, Data function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Function<Long,Long> f = nodeEngine.toObject(function);
        AtomicLongWrapper number = getNumber();

        long input = number.get();
        response = input;
        long output = f.apply(input);
        shouldBackup = input!=output;
        if(shouldBackup){
            backup = output;
            number.set(output);
        }
    }
}
