/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializationUtilTest {

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
        SerializationUtil.createSerializerAdapter(new InvalidSerializer(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPortableVersion_negativeVersion() {
        SerializationUtil.getPortableVersion(new DummyVersionedPortable(), 1);
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
