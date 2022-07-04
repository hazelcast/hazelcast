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

package com.hazelcast.sql.impl;

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
public class QueryIdTest extends CoreSqlTestSupport {
    @Test
    public void testIds() {
        UUID memberId = UUID.randomUUID();
        UUID localId = UUID.randomUUID();

        QueryId id = create(memberId, localId);

        assertEquals(memberId, id.getMemberId());
        assertEquals(localId, id.getLocalId());
    }

    @Test
    public void testEquals() {
        UUID memberId1 = UUID.randomUUID();
        UUID memberId2 = UUID.randomUUID();
        UUID localId1 = UUID.randomUUID();
        UUID localId2 = UUID.randomUUID();

        checkEquals(create(memberId1, localId1), create(memberId1, localId1), true);
        checkEquals(create(memberId1, localId1), create(memberId1, localId2), false);
        checkEquals(create(memberId1, localId1), create(memberId2, localId1), false);
        checkEquals(create(memberId1, localId1), create(memberId2, localId2), false);
    }

    @Test
    public void testMemberId() {
        UUID memberId = UUID.randomUUID();

        QueryId id1 = QueryId.create(memberId);
        QueryId id2 = QueryId.create(memberId);

        assertEquals(memberId, id2.getMemberId());
        assertEquals(id1.getMemberId(), id2.getMemberId());
        assertNotEquals(id1.getLocalId(), id2.getLocalId());
    }

    @Test
    public void testSerialization() {
        QueryId original = QueryId.create(UUID.randomUUID());
        QueryId restored = serializeAndCheck(original, SqlDataSerializerHook.QUERY_ID);

        assertEquals(original, restored);
    }

    @Test
    public void testParsing() {
        QueryId original = QueryId.create(UUID.randomUUID());
        QueryId restored = QueryId.parse(original.toString());
        assertEquals(original, restored);

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID().toString());
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID() + "_");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID() + "!_" + UUID.randomUUID());
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID() + "_" + UUID.randomUUID() + "!");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID() + "_" + UUID.randomUUID() + "_");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QueryId.parse(UUID.randomUUID() + "_" + UUID.randomUUID() + "_" + UUID.randomUUID());
        });
    }

    private static QueryId create(UUID memberId, UUID localId) {
        return new QueryId(memberId.getMostSignificantBits(), memberId.getLeastSignificantBits(),
            localId.getMostSignificantBits(), localId.getLeastSignificantBits());
    }
}
