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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import org.junit.Test;

import static org.junit.Assert.*;

public class ScheduledExecutorWaitNotifyKeyTest {

    @Test
    public void equals_sameRef()
            throws Exception {

        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handler.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);

        assertTrue(keyA.equals(keyA));
    }

    @Test
    public void equals_sameURN()
            throws Exception {

        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handler.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);
        ScheduledExecutorWaitNotifyKey keyB = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);

        assertTrue(keyA.equals(keyB));
    }

    @Test
    public void equals_fail_withNull()
            throws Exception {

        ScheduledTaskHandler handlerA = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handlerA.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);

        assertFalse(keyA.equals(null));
    }

    @Test
    public void equals_fail_withDiffURN()
            throws Exception {

        ScheduledTaskHandler handlerA = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handlerA.toUrn();

        ScheduledTaskHandler handlerB = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask2");
        String myTask2URN = handlerB.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);
        ScheduledExecutorWaitNotifyKey keyB = new ScheduledExecutorWaitNotifyKey("myScheduler", myTask2URN);

        assertFalse(keyA.equals(keyB));
    }

    @Test
    public void equals_fail_withDiffDistObject()
            throws Exception {

        ScheduledTaskHandler handlerA = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handlerA.toUrn();

        ScheduledTaskHandler handlerB = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask2");
        String myTask2URN = handlerB.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);
        ScheduledExecutorWaitNotifyKey keyB = new ScheduledExecutorWaitNotifyKey("myWrongScheduler", myTask2URN);

        assertFalse(keyA.equals(keyB));
    }


    @Test
    public void hashcode_sameRef()
            throws Exception {

        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handler.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);

        assertEquals(keyA.hashCode(), keyA.hashCode());
    }

    @Test
    public void hashcode_sameURN()
            throws Exception {

        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handler.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);
        ScheduledExecutorWaitNotifyKey keyB = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);

        assertEquals(keyA.hashCode(), keyB.hashCode());
    }

    @Test
    public void hashcode_fail_withDiffURN()
            throws Exception {

        ScheduledTaskHandler handlerA = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask");
        String myTaskURN = handlerA.toUrn();

        ScheduledTaskHandler handlerB = ScheduledTaskHandlerImpl.of(1, "MyScheduler", "MyTask2");
        String myTask2URN = handlerB.toUrn();

        ScheduledExecutorWaitNotifyKey keyA = new ScheduledExecutorWaitNotifyKey("myScheduler", myTaskURN);
        ScheduledExecutorWaitNotifyKey keyB = new ScheduledExecutorWaitNotifyKey("myScheduler", myTask2URN);

        assertNotEquals(keyA.hashCode(), keyB.hashCode());
    }
}
