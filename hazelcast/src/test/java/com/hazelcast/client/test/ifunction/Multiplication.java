package com.hazelcast.client.test.ifunction;

import com.hazelcast.client.test.IdentifiedFactory;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class Multiplication implements IFunction<Long, Long>, IdentifiedDataSerializable {
    public static final int CLASS_ID = 16;
    private long multiplier;

    @Override
    public Long apply(Long input) {
        return input * multiplier;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(multiplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        multiplier = in.readLong();
    }
}
