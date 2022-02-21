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
public class RecentlyActiveSplitBrainProtectionFunctionTest extends AbstractSplitBrainProtectionFunctionTest {

    @Test
    public void testRecentlyActiveSplitBrainProtectionFunction_splitBrainProtectionPresent_allMembersRecentlyActive() {
        splitBrainProtectionFunction = new RecentlyActiveSplitBrainProtectionFunction(splitBrainProtectionSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    @Test
    public void testRecentlyActiveSplitBrainProtectionFunction_splitBrainProtectionPresent_whenAsManyAsSplitBrainProtectionRecentlyActive() {
        splitBrainProtectionFunction = new RecentlyActiveSplitBrainProtectionFunction(splitBrainProtectionSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        assertTrue(splitBrainProtectionFunction.apply(subsetOfMembers(splitBrainProtectionSize)));
    }

    @Test
    public void testRecentlyActiveSplitBrainProtectionFunction_splitBrainProtectionPresent_whenFewerThanSplitBrainProtectionLive() {
        splitBrainProtectionFunction = new RecentlyActiveSplitBrainProtectionFunction(splitBrainProtectionSize, 10000);
        // heartbeat each second for all members for 5 seconds
        heartbeat(5, 1000);
        // cluster membership manager considers splitBrainProtectionSize-1 members alive
        assertFalse(splitBrainProtectionFunction.apply(subsetOfMembers(splitBrainProtectionSize - 1)));
    }

    @Test
    public void testSplitBrainProtectionAbsent_whenHeartbeatsReceivedBeforeToleratedWindow() throws Exception {
        // will do 5 heartbeats with 500msec interval starting from now
        long now = System.currentTimeMillis();
        // initialize clock offset +1 minute
        long clockOffset = 60000;
        initClockOffsetTest(clockOffset);
        // split brain protection function requires heartbeat within last 10 seconds
        createSplitBrainProtectionFunctionProxy(splitBrainProtectionSize, 10000);
        heartbeat(now, 5, 500);
        assertFalse(splitBrainProtectionFunction.apply(Arrays.asList(members)));
    }

    private void createSplitBrainProtectionFunctionProxy(int splitBrainProtectionSize, int toleranceMillis) throws Exception {
        Object qfOnTargetClassLoader = createSplitBrainProtectionFunctionReflectively(splitBrainProtectionSize, toleranceMillis);
        splitBrainProtectionFunction = (SplitBrainProtectionFunction)
                proxyObjectForStarter(RecentlyActiveSplitBrainProtectionFunctionTest.class.getClassLoader(), qfOnTargetClassLoader);
    }

    private Object createSplitBrainProtectionFunctionReflectively(int splitBrainProtectionSize, int toleranceMillis) {
        try {
            Class<?> splitBrainProtectionClazz = filteringClassloader.loadClass("com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction");
            Constructor<?> constructor = splitBrainProtectionClazz.getDeclaredConstructor(Integer.TYPE, Integer.TYPE);

            Object[] args = new Object[]{splitBrainProtectionSize, toleranceMillis};
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Could not create new instance of RecentlyActiveSplitBrainProtectionFunction reflectively", e);
        }
    }
}
