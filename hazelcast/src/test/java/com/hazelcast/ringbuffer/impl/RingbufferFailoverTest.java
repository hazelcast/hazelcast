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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.ringbuffer.impl.RingbufferAbstractTest.initAndGetConfig;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferFailoverTest extends HazelcastTestSupport {

    @Test
    public void sizeShouldNotExceedCapacity_whenPromotedFromBackup() {
        Config config = initAndGetConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance primaryInstance = factory.newHazelcastInstance(config);
        HazelcastInstance backupInstance = factory.newHazelcastInstance(config);

        String name = HazelcastTestSupport.randomNameOwnedBy(primaryInstance, getTestMethodName());
        Ringbuffer<String> ringbuffer = backupInstance.getRingbuffer(name);

        for (int i = 0; i < 100; i++) {
            ringbuffer.add(randomString());
        }

        waitAllForSafeState(factory.getAllHazelcastInstances());
        primaryInstance.getLifecycleService().terminate();

        assertEquals(ringbuffer.capacity(), ringbuffer.size());
    }
}
