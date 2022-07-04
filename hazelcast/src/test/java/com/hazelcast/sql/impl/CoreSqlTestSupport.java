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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Helper test classes.
 */
public class CoreSqlTestSupport extends HazelcastTestSupport {

    @ClassRule
    public static OverridePropertyRule enableJetRule = OverridePropertyRule.set("hz.jet.enabled", "true");

    /**
     * Check object equality with additional hash code check.
     *
     * @param first    First object.
     * @param second   Second object.
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

    public static <T> T serializeAndCheck(Object original, int expectedClassId) {
        assertTrue(original instanceof IdentifiedDataSerializable);

        IdentifiedDataSerializable original0 = (IdentifiedDataSerializable) original;

        assertEquals(SqlDataSerializerHook.F_ID, original0.getFactoryId());
        assertEquals(expectedClassId, original0.getClassId());

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        return ss.toObject(ss.toData(original));
    }
}
