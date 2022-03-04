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

package com.hazelcast.splitbrainprotection.impl;

import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ProbabilisticSplitBrainProtectionFunctionTest extends AbstractSplitBrainProtectionFunctionTest {

    @Test
    public void testSplitBrainProtectionPresent_whenAllMembersAlive() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testSplitBrainProtectionPresent_whenAsManyAsSplitBrainProtectionPresent() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        heartbeat(5, 1000);
        assertTrue(splitBrainProtectionFunction.apply(subsetOfMembers(splitBrainProtectionSize)));
    }

    @Test
    public void testSplitBrainProtectionAbsent_whenFewerThanSplitBrainProtectionPresent() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        heartbeat(5, 1000);
        // cluster membership manager considers fewer than split brain protection members live
        assertFalse(splitBrainProtectionFunction.apply(subsetOfMembers(splitBrainProtectionSize - 1)));
    }

    @Test
    public void testSplitBrainProtectionAbsent_whenHeartbeatsLate() throws Exception {
        // will do 5 heartbeats with 500msec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +1 minute --> last heartbeat received was too far in the past
        long clockOffset = 60000;
        initClockOffsetTest(clockOffset);
        createSplitBrainProtectionFunctionProxy(1000, 1000);
        heartbeat(now, 5, 500);
        assertFalse(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testSplitBrainProtectionPresent_whenHeartbeatsOnTime() throws Exception {
        // will do 5 heartbeats with 1 sec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +5seconds -> last heartbeat has just been received
        long clockOffset = 5 * 1000;
        initClockOffsetTest(clockOffset);
        createSplitBrainProtectionFunctionProxy(5000, 1000);
        heartbeat(5, 1000);
        assertTrue(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testSplitBrainProtectionAbsent_whenIcmpSuspects() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        prepareSplitBrainProtectionFunctionForIcmpFDTest(splitBrainProtectionFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingFailure();

        assertFalse(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testSplitBrainProtectionPresent_whenIcmpAndHeartbeatAlive() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        prepareSplitBrainProtectionFunctionForIcmpFDTest(splitBrainProtectionFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingSuccessfully();

        assertTrue(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testSplitBrainProtectionAbsent_whenIcmpAlive_andFewerThanSplitBrainProtectionPresent() {
        splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(splitBrainProtectionSize, 10000, 10000, 200, 100, 10);
        prepareSplitBrainProtectionFunctionForIcmpFDTest(splitBrainProtectionFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingSuccessfully();

        assertFalse(splitBrainProtectionFunction.apply(subsetOfMembers(splitBrainProtectionSize - 1)));
    }

    private void createSplitBrainProtectionFunctionProxy(long maxNoHeartbeatMillis, long hearbeatIntervalMillis) throws Exception {
        Object qfOnTargetClassLoader = createSplitBrainProtectionFunctionReflectively(maxNoHeartbeatMillis, hearbeatIntervalMillis, splitBrainProtectionSize);
        splitBrainProtectionFunction = (SplitBrainProtectionFunction)
                proxyObjectForStarter(ProbabilisticSplitBrainProtectionFunctionTest.class.getClassLoader(), qfOnTargetClassLoader);
    }

    private Object createSplitBrainProtectionFunctionReflectively(long maxNoHeartbeatMillis, long hearbeatIntervalMillis, int splitBrainProtectionSize) {
        try {
            Class<?> splitBrainProtectionClazz = filteringClassloader.loadClass("com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction");
            Constructor<?> constructor = splitBrainProtectionClazz.getDeclaredConstructor(Integer.TYPE, Long.TYPE, Long.TYPE, Integer.TYPE,
                    Long.TYPE, Double.TYPE);

            Object[] args = new Object[]{splitBrainProtectionSize, hearbeatIntervalMillis, maxNoHeartbeatMillis, 200, 100, 10};
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Could not setup clock offset", e);
        }
    }
}
