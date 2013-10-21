package com.hazelcast.web;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map.Entry;

public class SessionAttributePredicate implements Predicate, DataSerializable {
    private String sessionId;

    // Serialization Constructor
    public SessionAttributePredicate() {
    }

    public SessionAttributePredicate(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public boolean apply(Entry mapEntry) {
        Object key = mapEntry.getKey();
        if (key instanceof String) {
            String k = (String) key;
            return k.startsWith(sessionId + WebFilter.HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR);
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
