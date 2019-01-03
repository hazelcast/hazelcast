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
public class RecentlyActiveQuorumFunctionTest extends AbstractQuorumFunctionTest {

    @Test
    public void testRecentlyActiveQuorumFunction_quorumPresent_allMembersRecentlyActive() {
        quorumFunction = new RecentlyActiveQuorumFunction(quorumSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(quorumFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testRecentlyActiveQuorumFunction_quorumPresent_whenAsManyAsQuorumRecentlyActive() {
        quorumFunction = new RecentlyActiveQuorumFunction(quorumSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(quorumFunction.apply(subsetOfMembers(quorumSize)));
    }

    @Test
    public void testRecentlyActiveQuorumFunction_quorumPresent_whenFewerThanQuorumLive() {
        quorumFunction = new RecentlyActiveQuorumFunction(quorumSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        // cluster membership manager considers quorumSize-1 members alive
        assertFalse(quorumFunction.apply(subsetOfMembers(quorumSize - 1)));
    }

    @Test
    public void testQuorumAbsent_whenHeartbeatsReceivedBeforeToleratedWindow() throws Exception {
        // will do 5 heartbeats with 500msec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +1 minute
        long clockOffset = 60000;
        initClockOffsetTest(clockOffset);
        // quorum function requires heartbeat within last 10 seconds
        createQuorumFunctionProxy(quorumSize, 10000);
        heartbeat(now, 5, 500);
        assertFalse(quorumFunction.apply(Arrays.asList(members)));
    }

    private void createQuorumFunctionProxy(int quorumSize, int toleranceMillis) throws Exception {
        Object qfOnTargetClassLoader = createQuorumFunctionReflectively(quorumSize, toleranceMillis);
        quorumFunction = (QuorumFunction)
                proxyObjectForStarter(RecentlyActiveQuorumFunctionTest.class.getClassLoader(), qfOnTargetClassLoader);
    }

    private Object createQuorumFunctionReflectively(int quorumSize, int toleranceMillis) {
        try {
            Class<?> quorumClazz = filteringClassloader.loadClass("com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction");
            Constructor<?> constructor = quorumClazz.getDeclaredConstructor(Integer.TYPE, Integer.TYPE);

            Object[] args = new Object[]{quorumSize, toleranceMillis};
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Could not create new instance of RecentlyActiveQuorumFunction reflectively", e);
        }
    }
}
