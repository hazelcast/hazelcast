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
package com.hazelcast.client;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.ClientVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientVersionTest {

    @Test
    public void testStringFactory() {
        assertEquals(ClientVersion.of(1, 2, 3), ClientVersion.of("1.2.3"));
        assertEquals(ClientVersion.of(2, 0, 0), ClientVersion.of("2.0"));
        assertEquals(ClientVersion.of(2, 0, 1), ClientVersion.of("2.0.1-SNAPSHOT"));
        assertEquals(ClientVersion.of(2, 0, 1), ClientVersion.of("2.0.1-PREVIEW1-SNAPSHOT"));
        assertEquals(ClientVersion.of(2, 0, 1), ClientVersion.of("2.0.1-PREVIEW1"));
    }

    @Test
    public void testComparator() {
        assertEquals(0, ClientVersion.of(1, 2, 3).compareTo(ClientVersion.of(1, 2, 3)));
        assertTrue(ClientVersion.of(2, 2, 3).compareTo(ClientVersion.of(1, 2, 3)) > 0);
        assertTrue(ClientVersion.of(1, 3, 3).compareTo(ClientVersion.of(1, 2, 3)) > 0);
        assertTrue(ClientVersion.of(1, 2, 4).compareTo(ClientVersion.of(1, 2, 3)) > 0);
        assertTrue(ClientVersion.of(1, 2, 3).compareTo(ClientVersion.of(2, 2, 3)) < 0);
        assertTrue(ClientVersion.of(1, 2, 3).compareTo(ClientVersion.of(1, 3, 3)) < 0);
        assertTrue(ClientVersion.of(1, 2, 3).compareTo(ClientVersion.of(1, 2, 4)) < 0);
    }

    @Test
    public void testToString() {
        assertEquals(ClientVersion.of(1, 2, 3).toString(), "1.2.3");
    }

}
