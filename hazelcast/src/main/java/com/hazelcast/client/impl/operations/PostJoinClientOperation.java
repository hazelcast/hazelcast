package com.hazelcast.client.impl.operations;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PostJoinClientOperation extends AbstractOperation {

    private Map<String, String> mappings;

    public PostJoinClientOperation() {
    }

    public PostJoinClientOperation(Map<String, String> mappings) {
        this.mappings = mappings;
    }

    @Override
    public void run() throws Exception {
        if (mappings != null) {
            ClientEngineImpl engine = getService();
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                engine.addOwnershipMapping(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (mappings == null) {
            out.writeInt(0);
            return;
        }
        int len = mappings.size();
        out.writeInt(len);
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        mappings = new HashMap<String, String>(len);
        for (int i = 0; i < len; i++) {
            String clientUuid = in.readUTF();
            String ownerUuid = in.readUTF();
            mappings.put(clientUuid, ownerUuid);
        }
    }
}
