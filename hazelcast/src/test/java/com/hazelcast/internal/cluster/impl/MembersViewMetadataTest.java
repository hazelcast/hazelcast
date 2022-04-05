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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembersViewMetadataTest {

    @Test
    public void equalsAndHashCode() throws Exception {
        UUID memberUUID = new UUID(1, 1);
        MembersViewMetadata metadata
                = new MembersViewMetadata(new Address("localhost", 1234), memberUUID, new Address("localhost", 4321), 0);

        assertEqualAndHashCode(metadata, metadata);
        assertNotEquals(metadata, null);
        assertNotEquals(metadata, "");
        assertEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), memberUUID, new Address("localhost", 4321), 0));

        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 999), memberUUID, new Address("localhost", 4321), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), UUID.randomUUID(), new Address("localhost", 4321), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), memberUUID, new Address("localhost", 999), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), memberUUID, new Address("localhost", 4321), 999));
    }

    private static void assertEqualAndHashCode(Object o1, Object o2) {
        assertEquals(o1, o2);
        assertEquals(o1.hashCode(), o2.hashCode());
    }

    private static void assertNotEqualAndHashCode(Object o1, Object o2) {
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
    }
}
