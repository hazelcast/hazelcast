/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionServiceTest {

    ExecutionService es;

    @Before
    public void before() {
        HazelcastInstance hzMock = mock(HazelcastInstance.class);
        Mockito.when(hzMock.getName()).thenReturn("test-hz-instance");
        es = new ExecutionService(hzMock, "test-execservice", new JetEngineConfig().setParallelism(4));
    }

    @Test
    public void when_blockingTask_then_executed() throws Exception {
        final MockTasklet t = new MockTasklet().blocking();
        es.execute(singletonList(t)).get();
        assertTrue(t.didRun);
    }

    @Test
    public void when_nonblockingTask_then_executed() throws Exception {
        final MockTasklet t = new MockTasklet();
        es.execute(singletonList(t)).get();
        assertTrue(t.didRun);
    }

    @Test(expected = ExecutionException.class)
    public void when_nonblockingAndInitFails_then_futureFails() throws Exception {
        final MockTasklet t = new MockTasklet().initFails();
        es.execute(singletonList(t)).get();
    }

    @Test(expected = ExecutionException.class)
    public void when_blockingAndInitFails_then_futureFails() throws Exception {
        final MockTasklet t = new MockTasklet().blocking().initFails();
        es.execute(singletonList(t)).get();
    }

    @Test(expected = ExecutionException.class)
    public void when_nonblockingAndCallFails_then_futureFails() throws Exception {
        final MockTasklet t = new MockTasklet().callFails();
        es.execute(singletonList(t)).get();
    }

    @Test(expected = ExecutionException.class)
    public void when_blockingAndCallFails_then_futureFails() throws Exception {
        final MockTasklet t = new MockTasklet().blocking().callFails();
        es.execute(singletonList(t)).get();
    }

    static class MockTasklet implements Tasklet {

        volatile boolean didRun;

        boolean isBlocking;
        boolean initFails;
        boolean callFails;

        @Override
        public boolean isBlocking() {
            return isBlocking;
        }

        @Override
        public ProgressState call() {
            didRun = true;
            if (callFails) {
                throw new RuntimeException("mock call failure");
            }
            return ProgressState.DONE;
        }

        @Override
        public void init() {
            if (initFails) {
                throw new RuntimeException("mock init failure");
            }
        }

        MockTasklet blocking() {
            isBlocking = true;
            return this;
        }

        MockTasklet initFails() {
            initFails = true;
            return this;
        }

        MockTasklet callFails() {
            callFails = true;
            return this;
        }
    }
}
