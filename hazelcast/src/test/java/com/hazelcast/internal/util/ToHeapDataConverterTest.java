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

package com.hazelcast.internal.util;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ToHeapDataConverterTest {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ToHeapDataConverter.class);
    }

    @Test
    public void toHeapData() throws Exception {
        Data data = ToHeapDataConverter.toHeapData(new AnotherDataImpl());
        assertInstanceOf(HeapData.class, data);
    }

    @Test
    public void whenNull() throws Exception {
        Data data = ToHeapDataConverter.toHeapData(null);
        assertNull(data);
    }

    class AnotherDataImpl implements Data {

        @Override
        public byte[] toByteArray() {
            return new byte[0];
        }

        @Override
        public int getType() {
            return 0;
        }

        @Override
        public void copyTo(byte[] dest, int destPos) {
        }

        @Override
        public int totalSize() {
            return 0;
        }

        @Override
        public int dataSize() {
            return 0;
        }

        @Override
        public int getHeapCost() {
            return 0;
        }

        @Override
        public int getPartitionHash() {
            return 0;
        }

        @Override
        public boolean hasPartitionHash() {
            return false;
        }

        @Override
        public long hash64() {
            return 0;
        }

        @Override
        public boolean isPortable() {
            return false;
        }

        @Override
        public boolean isJson() {
            return false;
        }

        @Override
        public boolean isCompact() {
            return false;
        }
    }
}
