/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializationUtilTest {

    private final InternalSerializationService mockSs = mock(InternalSerializationService.class);

    @Test
    public void testIsNullData() {
        Assert.assertTrue(SerializationUtil.isNullData(new HeapData()));
    }

    @Test(expected = Error.class)
    public void testHandleException_OOME() {
        SerializationUtil.handleException(new OutOfMemoryError());
    }

    @Test(expected = Error.class)
    public void testHandleException_otherError() {
        SerializationUtil.handleException(new UnknownError());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSerializerAdapter_invalidSerializer() {
        SerializationUtil.createSerializerAdapter(new InvalidSerializer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPortableVersion_negativeVersion() {
        SerializationUtil.getPortableVersion(new DummyVersionedPortable(), 1);
    }

    @Test
    public void testReadWriteNullableBoolean_whenNull()
            throws IOException {
        byte[] bytes = serialize(null);
        assertNull(deserialize(bytes));
    }

    @Test
    public void testReadWriteNullableBoolean_whenFalse()
            throws IOException {
        byte[] bytes = serialize(false);
        assertFalse(deserialize(bytes));
    }

    @Test
    public void testReadWriteNullableBoolean_whenTrue()
            throws IOException {
        byte[] bytes = serialize(true);
        assertTrue(deserialize(bytes));
    }

    @Test(expected = IllegalStateException.class)
    public void testReadWriteNullableBoolean_whenInvalid()
            throws IOException {
        byte[] bytes = new byte[1];
        bytes[0] = 55;
        deserialize(bytes);
    }

    private byte[] serialize(Boolean b) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, mockSs);
        SerializationUtil.writeNullableBoolean(out, b);
        return bout.toByteArray();
    }

    private Boolean deserialize(byte[] bytes) throws IOException {
        ObjectDataInput in = createObjectDataInputStream(new ByteArrayInputStream(bytes), mockSs);
        return SerializationUtil.readNullableBoolean(in);
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
