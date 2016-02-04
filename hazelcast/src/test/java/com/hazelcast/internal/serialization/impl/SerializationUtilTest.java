package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.asStreamSerializer;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.getPortableVersion;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.isNullData;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SerializationUtilTest {

    @Test
    public void testIsNullData() throws Exception {
        assertTrue(isNullData(new HeapData()));
    }

    @Test(expected = Error.class)
    public void testHandleException_OOME() throws Exception {
        handleException(new OutOfMemoryError());
    }

    @Test(expected = Error.class)
    public void testHandleException_otherError() throws Exception {
        handleException(new UnknownError());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAsStreamSerializer_invalidSerializer() throws Exception {
        asStreamSerializer(new InvalidSerializer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPortableVersion_negativeVersion() throws Exception {
        getPortableVersion(new DummyVersionedPortable(), 1);
    }

    private class InvalidSerializer implements Serializer {
        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void destroy() {
        }
    }

    private class DummyVersionedPortable implements VersionedPortable {
        @Override
        public int getClassVersion() {
            return -1;
        }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
        }

    }
} 
