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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorWaitNotifyKeyTest {

    private DurableExecutorWaitNotifyKey notifyKey;
    private DurableExecutorWaitNotifyKey notifyKeySameAttributes;
    private DurableExecutorWaitNotifyKey notifyKeyOtherUniqueId;
    private DurableExecutorWaitNotifyKey notifyKeyOtherName;

    @Before
    public void setUp() {
        String name = "objectName";
        String otherName = "otherObjectName";

        notifyKey = new DurableExecutorWaitNotifyKey(name, 23);
        notifyKeySameAttributes = new DurableExecutorWaitNotifyKey(name, 23);
        notifyKeyOtherUniqueId = new DurableExecutorWaitNotifyKey(name, 42);
        notifyKeyOtherName = new DurableExecutorWaitNotifyKey(otherName, 23);
    }

    @Test
    public void testEquals() {
        assertEquals(notifyKey, notifyKey);
        assertEquals(notifyKey, notifyKeySameAttributes);

        assertNotEquals(notifyKey, null);
        assertNotEquals(notifyKey, new Object());

        assertNotEquals(notifyKey, notifyKeyOtherUniqueId);
        assertNotEquals(notifyKey, notifyKeyOtherName);
    }

    @Test
    public void testHashCode() {
        assertEquals(notifyKey.hashCode(), notifyKey.hashCode());
        assertEquals(notifyKey.hashCode(), notifyKeySameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(notifyKey.hashCode(), notifyKeyOtherUniqueId.hashCode());
        assertNotEquals(notifyKey.hashCode(), notifyKeyOtherName.hashCode());
    }
}
