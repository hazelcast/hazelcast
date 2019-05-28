/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.nio.Address;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.ScheduledTaskHandlerImplConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledTaskHandlerImplConstructorTest {

    @Test
    public void testConstructor() throws Exception {
        String schedulerName = "myScheduler";
        String taskName = "myTask";
        int partitionId = 23;
        Address address = new Address("172.16.16.1", 4223);

        ScheduledTaskHandler handlerWithAddress = ScheduledTaskHandlerImpl.of(address, schedulerName, taskName);
        ScheduledTaskHandler handlerWithPartitionId = ScheduledTaskHandlerImpl.of(partitionId, schedulerName, taskName);

        ScheduledTaskHandlerImplConstructor constructor = new ScheduledTaskHandlerImplConstructor(ScheduledTaskHandlerImpl.class);
        ScheduledTaskHandler clonedHandlerWithAddress = (ScheduledTaskHandler) constructor.createNew(handlerWithAddress);
        ScheduledTaskHandler clonedHandlerWithPartitionId = (ScheduledTaskHandler) constructor.createNew(handlerWithPartitionId);

        assertThatScheduledTaskHandlerAreEqual(handlerWithAddress, clonedHandlerWithAddress);
        assertThatScheduledTaskHandlerAreEqual(handlerWithPartitionId, clonedHandlerWithPartitionId);
    }

    private static void assertThatScheduledTaskHandlerAreEqual(ScheduledTaskHandler expected, ScheduledTaskHandler actual) {
        assertEquals(expected.getAddress(), actual.getAddress());
        assertEquals(expected.getSchedulerName(), actual.getSchedulerName());
        assertEquals(expected.getTaskName(), actual.getTaskName());
        assertEquals(expected.getPartitionId(), actual.getPartitionId());
    }
}
