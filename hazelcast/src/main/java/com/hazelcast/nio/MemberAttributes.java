package com.hazelcast.nio;

import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: grant
 */
public class MemberAttributes implements DataSerializable {

    private Map<String, String> attributes;

    public MemberAttributes(Collection<MemberAttributeConfig> configs) {
        this.attributes = new ConcurrentHashMap<String, String>();
        for (MemberAttributeConfig memberAttributeConfig : configs) {
            attributes.put(memberAttributeConfig.getName(), memberAttributeConfig.getValue());
        }
    }

    public MemberAttributes() {
        this.attributes = new ConcurrentHashMap<String, String>();
    }

    public MemberAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public <T> T getValue(String key) {
        return (T)attributes.get(key);
    }

    public void addAttribute(String key, String value) {
        attributes.put(key, value);
    }

    public <T> T removeAttribute(String key) {
        return (T)attributes.remove(key);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(attributes != null);
        if (null != attributes) {
            out.writeInt(attributes.size());
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            int size = in.readInt();
            Map<String, String> attributes = new HashMap<String, String>(size);
            for (int x=0; x<size; x++) {
                attributes.put(in.readUTF(), in.readUTF());
            }
            this.attributes = attributes;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberAttributes that = (MemberAttributes) o;

        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return attributes != null ? attributes.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MemberAttributes{" +
                "attributes=" + attributes +
                '}';
    }

    public int size() {
        return attributes.size();
    }
}
