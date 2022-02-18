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

package com.hazelcast.executor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * Reproduces issue: https://github.com/hazelcast/hazelcast/issues/5490
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutionCallbackAdapterRaceTest extends HazelcastTestSupport {

    private ILogger logger = Logger.getLogger(ExecutionCallbackAdapterFactory.class);

    private Member member1 = mock(Member.class);
    private Member member2 = mock(Member.class);

    private volatile boolean completed;
    private volatile boolean raceDetected;

    @Test
    public void test() throws Exception {
        MultiExecutionCallback callback = new MultiExecutionCallbackMock();

        final ExecutionCallbackAdapterFactory factory = new ExecutionCallbackAdapterFactory(logger, asList(member1, member2),
                callback);

        // first we spawn the response for the member1
        // this thread is going wait for 2 seconds in the onResponse to trigger the out of order behavior
        Future future = spawn(new Runnable() {
            @Override
            public void run() {
                factory.callbackFor(member1).onResponse("1");
            }
        });

        factory.callbackFor(member2).onResponse("2");

        future.get();

        assertFalse("Race was detected", raceDetected);
    }

    private class MultiExecutionCallbackMock implements MultiExecutionCallback {

        @Override
        public void onResponse(Member member, Object value) {
            if (member == member1) {
                sleepSeconds(2);
            }

            if (completed) {
                raceDetected = true;
            }
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            completed = true;
        }
    }
}
