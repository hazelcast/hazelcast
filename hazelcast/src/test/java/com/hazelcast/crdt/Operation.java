package com.hazelcast.crdt;

import java.util.List;
import java.util.Random;

public class Operation<T, S> {
    protected final Random rnd;
    protected final int crdtIndex;

    public Operation(Random rnd) {
        this.rnd = rnd;
        this.crdtIndex = rnd.nextInt(10000);
    }

    protected T getCRDT(List<T> crdts) {
        return crdts.get(crdtIndex % crdts.size());
    }

    protected void perform(List<T> crdts, S state) {
        perform(getCRDT(crdts), state);
    }

    protected void perform(T crdt, S state) {
        // intended for override
    }
}