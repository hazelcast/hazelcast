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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Helper test classes.
 */
public class SqlTestSupport extends HazelcastTestSupport {
    /**
     * Check object equality with additional hash code check.
     *
     * @param first First object.
     * @param second Second object.
     * @param expected Expected result.
     */
    public static void checkEquals(Object first, Object second, boolean expected) {
        if (expected) {
            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
        } else {
            assertNotEquals(first, second);
        }
    }

    public static <T> T serialize(Object original) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        return ss.toObject(ss.toData(original));
    }

    public static <T> T serializeAndCheck(IdentifiedDataSerializable original, int expectedClassId) {
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(expectedClassId, original.getClassId());

        return serialize(original);
    }

    public static QueryFragmentContext emptyFragmentContext() {
        return emptyFragmentContext(Collections.emptyList());
    }

    public static QueryFragmentContext emptyFragmentContext(List<Object> args) {
        QueryState state = QueryState.createInitiatorState(
            QueryId.create(UUID.randomUUID()),
            UUID.randomUUID(),
            null,
            0L,
            null,
            null,
            null
        );

        return new QueryFragmentContext(args, new LoggingQueryFragmentScheduleCallback(), state);
    }
}
