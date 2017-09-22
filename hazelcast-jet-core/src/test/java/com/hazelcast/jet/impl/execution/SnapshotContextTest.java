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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.logging.ILogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SnapshotContextTest {

    private SnapshotContext ssContext =
            new SnapshotContext(mock(ILogger.class), 1, 1, 9, ProcessingGuarantee.EXACTLY_ONCE);

    @Before
    public void before() {
        ssContext.initTaskletCount(1, 0);
        ssContext.startNewSnapshot(10);
    }

    @After
    public void after() {
        assertEquals(0, ssContext.numRemainingTasklets.get());
    }

    @Test
    public void when_taskletDoneBeforeItDidCurrentSnapshot_then_countedAsSnapshotWasDone() {
        ssContext.taskletDone(9, false);
    }

    @Test
    public void when_taskletDoneAfterItDidCurrentSnapshot_then_countedAsSnapshotWasDone() {
        ssContext.snapshotDoneForTasklet();
        ssContext.taskletDone(10, false);
    }
}
