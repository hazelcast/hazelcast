package com.hazelcast.concurrent.atomicreference;

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
        Function f = nodeEngine.toObject(function);
        AtomicReferenceWrapper reference = getReference();

        Object input = nodeEngine.toObject(reference.get());
        Object output = f.apply(input);
        shouldBackup = !equals(input,output);
        if(shouldBackup){
            backup = nodeEngine.toData(output);
            reference.set(backup);
        }
    }
}

