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

package com.hazelcast.quorum.impl;

import com.hazelcast.quorum.QuorumFunction;
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
public class ProbabilisticQuorumFunctionTest extends AbstractQuorumFunctionTest {

    @Test
    public void testQuorumPresent_whenAllMembersAlive() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testQuorumPresent_whenAsManyAsQuorumPresent() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        heartbeat(5, 1000);
        assertTrue(quorumFunction.apply(subsetOfMembers(quorumSize)));
    }

    @Test
    public void testQuorumAbsent_whenFewerThanQuorumPresent() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        heartbeat(5, 1000);
        // cluster membership manager considers fewer than quorum members live
        assertFalse(quorumFunction.apply(subsetOfMembers(quorumSize - 1)));
    }

    @Test
    public void testQuorumAbsent_whenHeartbeatsLate() throws Exception {
        // will do 5 heartbeats with 500msec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +1 minute --> last heartbeat received was too far in the past
        long clockOffset = 60000;
        initClockOffsetTest(clockOffset);
        createQuorumFunctionProxy(1000, 1000);
        heartbeat(now, 5, 500);
        assertFalse(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testQuorumPresent_whenHeartbeatsOnTime() throws Exception {
        // will do 5 heartbeats with 1 sec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +5seconds -> last heartbeat has just been received
        long clockOffset = 5 * 1000;
        initClockOffsetTest(clockOffset);
        createQuorumFunctionProxy(5000, 1000);
        heartbeat(5, 1000);
        assertTrue(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testQuorumAbsent_whenIcmpSuspects() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        prepareQuorumFunctionForIcmpFDTest(quorumFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingFailure();

        assertFalse(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testQuorumPresent_whenIcmpAndHeartbeatAlive() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        prepareQuorumFunctionForIcmpFDTest(quorumFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingSuccessfully();

        assertTrue(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testQuorumAbsent_whenIcmpAlive_andFewerThanQuorumPresent() {
        quorumFunction = new ProbabilisticQuorumFunction(quorumSize, 10000, 10000, 200, 100, 10);
        prepareQuorumFunctionForIcmpFDTest(quorumFunction);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        pingSuccessfully();

        assertFalse(quorumFunction.apply(subsetOfMembers(quorumSize - 1)));
    }

    private void createQuorumFunctionProxy(long maxNoHeartbeatMillis, long hearbeatIntervalMillis) throws Exception {
        Object qfOnTargetClassLoader = createQuorumFunctionReflectively(maxNoHeartbeatMillis, hearbeatIntervalMillis, quorumSize);
        quorumFunction = (QuorumFunction)
                proxyObjectForStarter(ProbabilisticQuorumFunctionTest.class.getClassLoader(), qfOnTargetClassLoader);
    }

    private Object createQuorumFunctionReflectively(long maxNoHeartbeatMillis, long hearbeatIntervalMillis, int quorumSize) {
        try {
            Class<?> quorumClazz = filteringClassloader.loadClass("com.hazelcast.quorum.impl.ProbabilisticQuorumFunction");
            Constructor<?> constructor = quorumClazz.getDeclaredConstructor(Integer.TYPE, Long.TYPE, Long.TYPE, Integer.TYPE,
                    Long.TYPE, Double.TYPE);

            Object[] args = new Object[]{quorumSize, hearbeatIntervalMillis, maxNoHeartbeatMillis, 200, 100, 10};
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Could not setup clock offset", e);
        }
    }
}
