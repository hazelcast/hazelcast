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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BatchIMapEventTest {

    private BatchEventData batchEventData;
    private BatchIMapEvent batchIMapEvent;

    @Before
    public void setUp() throws Exception {
        batchEventData = new BatchEventData(Collections.<QueryCacheEventData>emptyList(), "source", 1);

        batchIMapEvent = new BatchIMapEvent(batchEventData);
    }

    @Test
    public void testGetBatchEventData() {
        assertEquals(batchEventData, batchIMapEvent.getBatchEventData());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMember() {
        batchIMapEvent.getMember();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetEventType() {
        batchIMapEvent.getEventType();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetName() {
        batchIMapEvent.getName();
    }
}
