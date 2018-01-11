/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryCloseConnections;
import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryShutdown;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OutOfMemoryHandlerHelperTest extends AbstractOutOfMemoryHandlerTest {

    @Before
    public void setUp() throws Exception {
        initHazelcastInstances();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(OutOfMemoryHandlerHelper.class);
    }

    @Test
    public void testTryCloseConnections() {
        tryCloseConnections(hazelcastInstance);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWithNullInstance() {
        tryCloseConnections(null);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWhenThrowableIsThrown() {
        tryCloseConnections(hazelcastInstanceThrowsException);
    }

    @Test
    public void testTryShutdown() {
        tryShutdown(hazelcastInstance);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWithNullInstance() {
        tryShutdown(null);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWhenThrowableIsThrown() {
        tryShutdown(hazelcastInstanceThrowsException);
    }
}
