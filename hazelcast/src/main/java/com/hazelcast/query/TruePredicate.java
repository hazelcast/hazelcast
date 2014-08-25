package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A {@link com.hazelcast.query.Predicate} which always returns true.
 */
public class TruePredicate implements DataSerializable, Predicate {

    /**
     * An instance of the TruePredicate.
     */
    public static final TruePredicate INSTANCE = new TruePredicate();

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return true;
    }

    @Override
    public boolean in(Predicate predicate) {
        return predicate instanceof TruePredicate;
    }
}
