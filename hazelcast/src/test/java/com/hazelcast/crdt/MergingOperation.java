package com.hazelcast.crdt;

import java.util.List;
import java.util.Random;

public class MergingOperation<C extends CRDT<C>, S> extends Operation<C, S> {
    private final int sourceIdx;

    MergingOperation(Random rnd) {
        super(rnd);
        this.sourceIdx = rnd.nextInt();
    }

    @Override
    protected void perform(List<C> crdts, S state) {
        final C target = getCRDT(crdts);
        int sourceIdx = rnd.nextInt(crdts.size());
        if (crdts.get(sourceIdx) == target) {
            sourceIdx = (sourceIdx + 1) % crdts.size();
        }
        target.merge(crdts.get(sourceIdx));
    }

    @Override
    public String toString() {
        return "Merge(" + crdtIndex + "," + sourceIdx + ")";
    }
}