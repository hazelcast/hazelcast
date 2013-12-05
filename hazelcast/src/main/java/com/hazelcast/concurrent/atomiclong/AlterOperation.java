package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;

public class AlterOperation extends AbstractAlterOperation {

    public AlterOperation() {
    }

    public AlterOperation(String name, Function<Long, Long> function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        AtomicLongWrapper reference = getNumber();

        long input = reference.get();
        long output = function.apply(input);
        shouldBackup = input != output;
        if (shouldBackup) {
            backup = output;
            reference.set(backup);
        }
    }
}

