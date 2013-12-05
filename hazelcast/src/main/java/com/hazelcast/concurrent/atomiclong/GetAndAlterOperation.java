package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;

public class GetAndAlterOperation extends AbstractAlterOperation {

    public GetAndAlterOperation() {
    }

    public GetAndAlterOperation(String name, Function<Long, Long> function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        AtomicLongWrapper number = getNumber();

        long input = number.get();
        response = input;
        long output = function.apply(input);
        shouldBackup = input != output;
        if (shouldBackup) {
            backup = output;
            number.set(output);
        }
    }
}
