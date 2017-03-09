/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.version;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionTest {

    @Test
    public void constituents() throws Exception {
        Version version = Version.of(3, 9);
        assertEquals(3, version.getMajor());
        assertEquals(9, version.getMinor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void ofMalformed() throws Exception {
        Version.of("3,9");
    }

    @Test
    public void equals() throws Exception {
        assertEquals(Version.UNKNOWN, Version.UNKNOWN);
        assertEquals(Version.of(3, 9), Version.of(3, 9));
        assertEquals(Version.of(3, 9), Version.of("3.9"));

        assertFalse(Version.of("3.8").equals(Version.of(4, 0)));
        assertFalse(Version.UNKNOWN.equals(Version.of(4, 1)));

        assertFalse(Version.UNKNOWN.equals(new Object()));
    }

    @Test
    public void compareTo() throws Exception {
        assertEquals(0, Version.of(3, 9).compareTo(Version.of(3, 9)));
        assertEquals(1, Version.of(3, 10).compareTo(Version.of(3, 9)));
        assertEquals(1, Version.of(4, 0).compareTo(Version.of(3, 9)));
        assertEquals(-1, Version.of(3, 9).compareTo(Version.of(3, 10)));
        assertEquals(-1, Version.of(3, 9).compareTo(Version.of(4, 10)));
    }

    @Test
    public void hashCodeTest() throws Exception {
        assertEquals(Version.UNKNOWN.hashCode(), Version.UNKNOWN.hashCode());

        assertTrue(Version.UNKNOWN.hashCode() != Version.of(3, 4).hashCode());
    }

    @Test
    public void testSerialization() {
        Version given = Version.of(3, 9);
        SerializationServiceV1 ss = new DefaultSerializationServiceBuilder().setVersion(SerializationServiceV1.VERSION_1).build();
        Version deserialized = ss.toObject(ss.toData(given));

        assertEquals(deserialized, given);
    }

    @Test
    public void toStringTest() throws Exception {
        assertEquals("3.8", Version.of(3, 8).toString());
    }
}
