package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AtomicReferenceReplicationOperation extends AbstractOperation {

    private Map<String, Data> migrationData;

    public AtomicReferenceReplicationOperation() {
        super();
    }

    public AtomicReferenceReplicationOperation(Map<String, Data> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        AtomicReferenceService atomicReferenceService = getService();
        for (Map.Entry<String, Data> entry : migrationData.entrySet()) {
            atomicReferenceService.getReference(entry.getKey()).set(entry.getValue());
        }
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Data> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, Data>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            Data data = in.readObject();
            migrationData.put(name, data);
        }
    }
}

