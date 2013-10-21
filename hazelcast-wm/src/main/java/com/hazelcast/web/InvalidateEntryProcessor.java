package com.hazelcast.web;

import java.io.IOException;
import java.util.Map.Entry;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class InvalidateEntryProcessor extends AbstractEntryProcessor<String, Object> implements DataSerializable {
    private String sessionId;
    
    // Serialization Constructor
    public InvalidateEntryProcessor() {
        super(true);
    }
    
    public InvalidateEntryProcessor(String sessionId) {
            this.sessionId = sessionId;
    }

    @Override
    public Object process(Entry<String, Object> entry) {
        Object key = entry.getKey();
        if (key instanceof String) {
            String k = (String) key;
            if (k.startsWith(sessionId + WebFilter.HAZELCAST_SESSION_ATTRIBUTE_SEPERATOR)) {
                entry.setValue(null);
            }
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(sessionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
            sessionId = in.readUTF();
    }
}
