/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.logstore;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.log.encoders.StringEncoder;
import com.hazelcast.log.encoders.UUIDEncoder;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ObjectLogStoreTest extends AbstractLogStoreTest {

    @Test
    public void testCreation() {
        ObjectLogStore<String> store = createObjectLogStore(new LogStoreConfig());

        Eden eden = store.eden;
        assertNotNull(eden);
        assertEquals(0, eden.address);
        assertNull(store.first);
        assertNull(store.last);
    }

    @Test
    public void testWriteObject_UUID_overManySegments() {
        LogStoreConfig config = new LogStoreConfig()
                .setSegmentSize(128)
                .setEncoder(new UUIDEncoder());
        ObjectLogStore<UUID> store = createObjectLogStore(config);

        long[] sequences = new long[16 * 1024];
        UUID[] items = new UUID[sequences.length];
        for (int k = 0; k < sequences.length; k++) {
            UUID value = UuidUtil.newUnsecureUUID();
            items[k] = value;
            sequences[k] = store.putObject(items[k]);
            assertEquals(value, store.getObject(sequences[k]));
        }

        for (int k = 0; k < sequences.length; k++) {
            UUID expected = items[k];
            long sequence = sequences[k];
            UUID found = store.getObject(sequence);
            assertEquals(expected, found);
        }
    }


    @Test
    public void testWriteObject_String_overManySegments() {
        LogStoreConfig config = new LogStoreConfig()
                .setSegmentSize(128)
                .setEncoder(new StringEncoder());
        ObjectLogStore<String> store = createObjectLogStore(config);

        long[] sequences = new long[16 * 1024];
        String[] items = new String[sequences.length];
        for (int k = 0; k < sequences.length; k++) {
            String value = "" + k;
            items[k] = value;
            sequences[k] = store.putObject(items[k]);
            assertEquals(value, store.getObject(sequences[k]));
        }

        for (int k = 0; k < sequences.length; k++) {
            String expected = items[k];
            long sequence = sequences[k];
            String found = store.getObject(sequence);
            assertEquals(expected, found);
        }
    }
}
