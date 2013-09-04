package com.hazelcast.spring;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

/**
 * @author asimarslan
 */
public class DummyPortableFactory implements PortableFactory{
    @Override
    public Portable create(int classId) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
