package com.hazelcast.spring;

import com.hazelcast.nio.serialization.Serializer;

/**
 * @author asimarslan
 */
public class DummyGlobalSerializationFactory implements Serializer {
    @Override
    public int getTypeId() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
