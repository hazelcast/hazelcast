package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;

public class AlterAndGetOperation extends AbstractAlterOperation {

    public AlterAndGetOperation() {
    }

    public AlterAndGetOperation(String name, Function<Long, Long> function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        AtomicLongWrapper number = getNumber();

        long input = number.get();
        long output = function.apply(input);
        shouldBackup = input != output;
        if (shouldBackup) {
            backup = output;
            number.set(output);
        }

        response = output;
    }
}
