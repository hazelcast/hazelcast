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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.mock.IAtomicLongMocks.mockIAtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IdGeneratorProxyTest {

    private IdGenerator idGenerator = createIdGenerator();

    @Test
    public void testInitIncrements() {
        assertNewIdAfterInit(0);
        assertNewIdAfterInit(1);
        assertNewIdAfterInit(10);
    }

    @Test
    public void testInitFailsOnNegativeValues() {
        assertFalse(idGenerator.init(-1));
    }

    private static void assertNewIdAfterInit(int initialValue) {
        IdGenerator idGenerator = createIdGenerator();

        assertTrue(idGenerator.init(initialValue));

        assertEquals(initialValue + 1, idGenerator.newId());
    }

    private static IdGenerator createIdGenerator() {
        String name = "id-" + UUID.randomUUID().toString();

        IAtomicLong blockGenerator = mockIAtomicLong();

        return new IdGeneratorProxy(blockGenerator, name, null, null);
    }
}
