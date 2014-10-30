package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link com.hazelcast.spi.OperationFactory} implementation for RemoveAll Operations.
 * <p>RemoveAll operation has two main purposes;
 * <ul>
 * <li>Remove all internal data
 * <li>Remove the entries of the provided keys.</li>
 * </ul></p>
 *
 * @see com.hazelcast.spi.OperationFactory
 */
public class CacheRemoveAllOperationFactory implements OperationFactory, IdentifiedDataSerializable {

    private String name;

    private Set<Data> keys;

    private int completionId;

    public CacheRemoveAllOperationFactory() {
    }

    public CacheRemoveAllOperationFactory(String name, Set<Data> keys, int completionId) {
        this.name = name;
        this.keys = keys;
        this.completionId = completionId;
    }

    @Override
    public Operation createOperation() {
        return new CacheRemoveAllOperation(name, keys, completionId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(completionId);
        out.writeInt(keys == null ? -1 : keys.size());
        if (keys != null) {
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        completionId = in.readInt();
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            keys.add(in.readData());
        }
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REMOVE_ALL_FACTORY;
    }
}
