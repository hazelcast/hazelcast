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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.hazelcast.jet2.impl.ProgressState.DONE;
import static com.hazelcast.jet2.impl.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionServiceTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

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

    @Test
    public void when_shutdown_then_submitFails() {
        es.execute(singletonList(new MockTasklet()));
        es.execute(singletonList(new MockTasklet()));
        es.shutdown();
        exceptionRule.expect(IllegalStateException.class);
        es.execute(singletonList(new MockTasklet()));
    }

    @Test
    public void when_manyCallsWithSomeStalling_then_eventuallyDone() throws Exception {
        final MockTasklet t1 = new MockTasklet().blocking().callsBeforeDone(10);
        final MockTasklet t2 = new MockTasklet().callsBeforeDone(10);
        es.execute(asList(t1, t2)).get();
    }

    @Test
    public void when_workStealing_then_allComplete() throws Exception {
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().callsBeforeDone(1000)).limit(100).collect(toList());
        es.execute(tasklets).get();
    }

    static class MockTasklet implements Tasklet {

        volatile boolean didRun;

        boolean isBlocking;
        boolean initFails;
        boolean callFails;
        int callsBeforeDone;

        private boolean willMakeProgress = true;

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
            willMakeProgress = !willMakeProgress;
            return callsBeforeDone-- == 0 ? DONE
                    : willMakeProgress ? MADE_PROGRESS
                    : NO_PROGRESS;
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

        MockTasklet callsBeforeDone(int count) {
            callsBeforeDone = count;
            return this;
        }
    }
}
