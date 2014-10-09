package com.hazelcast.multimap.impl.client;

import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.operations.ContainsEntryOperation;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * The KeyBasedContainsRequest can be used to determine if a key is available in a multimap (when value is null), or
 * to check if a map-entry is stored in the multimap (value not null).
 * <p/>
 * This request is 'cheap' since it will always be routed to a particular member in the cluster, unlike the
 * {@link ContainsRequest}.
 */
public class KeyBasedContainsRequest extends MultiMapKeyBasedRequest {

    private Data value;
    private long threadId;

    public KeyBasedContainsRequest() {
    }

    public KeyBasedContainsRequest(String name, Data key, Data value) {
        super(name, key);
        this.value = value;
    }

    public KeyBasedContainsRequest(String name, Data key, Data value, long threadId) {
        super(name, key);
        this.value = value;
        this.threadId = threadId;
    }

    @Override
    protected Operation prepareOperation() {
        ContainsEntryOperation operation = new ContainsEntryOperation(name, key, value);
        operation.setThreadId(threadId);
        return operation;
    }

    @Override
    public int getClassId() {
        return MultiMapPortableHook.KEY_BASED_CONTAINS;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("threadId", threadId);
        super.write(writer);
        IOUtil.writeNullableData(writer.getRawDataOutput(), value);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("threadId");
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        value = IOUtil.readNullableData(in);
    }

    @Override
    public String getMethodName() {
        if (value == null) {
            return "containsKey";
        }
        return "containsEntry";
    }

    @Override
    public Object[] getParameters() {
        if (value == null) {
            return new Object[]{key};
        }
        return new Object[]{key, value};
    }
}
