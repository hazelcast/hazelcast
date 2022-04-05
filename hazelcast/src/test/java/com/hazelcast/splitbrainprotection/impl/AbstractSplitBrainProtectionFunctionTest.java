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

import com.hazelcast.config.Config;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.splitbrainprotection.HeartbeatAware;
import com.hazelcast.splitbrainprotection.PingAware;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ClockProperties;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static java.lang.Thread.currentThread;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractSplitBrainProtectionFunctionTest
        extends HazelcastTestSupport {
    SplitBrainProtectionFunction splitBrainProtectionFunction;
    Member[] members;
    int splitBrainProtectionSize;
    int clusterSize;
    ClassLoader tccl;
    ClassLoader filteringClassloader;

    @Before
    public void setup() throws UnknownHostException {
        splitBrainProtectionSize = 3;
        clusterSize = 5;
        members = members(clusterSize);
    }

    @After
    public void tearDown() {
        cleanupClockOffsetTest();
    }

    // heartbeat count times with intervalMillis milliseconds in between heartbeats, starting at Clock.currentTimeMillis
    protected void heartbeat(int count, long intervalMillis) {
        long now = Clock.currentTimeMillis();
        heartbeat(now, count, intervalMillis);
    }

    protected void heartbeat(long now, int count, long intervalMillis) {
        for (int i = 0; i < count; i++) {
            long timestamp = now + (i * intervalMillis);
            for (Member member : members) {
                if (splitBrainProtectionFunction instanceof HeartbeatAware) {
                    ((HeartbeatAware) splitBrainProtectionFunction).onHeartbeat(member, timestamp);
                } else {
                    fail("Tested SplitBrainProtectionFunction should be heartbeat aware");
                }
            }
        }
    }

    protected void prepareSplitBrainProtectionFunctionForIcmpFDTest(SplitBrainProtectionFunction splitBrainProtectionFunction) {
        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getLocalMember()).thenReturn(members[0]);

        HazelcastInstance mockInstance = mock(HazelcastInstance.class);
        when(mockInstance.getConfig()).thenReturn(getIcmpFDEnabledConfig());
        when(mockInstance.getCluster()).thenReturn(mockCluster);

        assert splitBrainProtectionFunction instanceof HazelcastInstanceAware;
        ((HazelcastInstanceAware) splitBrainProtectionFunction).setHazelcastInstance(mockInstance);
    }

    protected void pingSuccessfully() {
        for (Member member : members) {
            if (splitBrainProtectionFunction instanceof PingAware) {
                ((PingAware) splitBrainProtectionFunction).onPingRestored(member);
            } else {
                fail("Tested SplitBrainProtectionFunction should be ping aware");
            }
        }
    }

    protected void pingFailure() {
        for (Member member : members) {
            if (splitBrainProtectionFunction instanceof PingAware) {
                for (int i = 0; i < 3; i++) {
                    ((PingAware) splitBrainProtectionFunction).onPingLost(member);
                }
            } else {
                fail("Tested SplitBrainProtectionFunction should be ping aware");
            }
        }
    }

    protected Config getIcmpFDEnabledConfig() {
        Config config = new Config();
        IcmpFailureDetectorConfig icmpFDConfig = new IcmpFailureDetectorConfig();
        icmpFDConfig.setEnabled(true)
                .setFailFastOnStartup(false)
                .setIntervalMilliseconds(1000)
                .setMaxAttempts(3)
                .setParallelMode(true);
        config.getNetworkConfig().setIcmpFailureDetectorConfig(icmpFDConfig);
        return config;
    }

    /**
     * Sets up the test to use Hazelcast classes off a separate classloader so that the clock offset can be set to any arbitrary
     * value. This is necessary for clock offset tests as {@link Clock} 's offset is initialized
     * in a static context.
     */
    protected void initClockOffsetTest(long offsetMillis) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offsetMillis));
        Thread thread = currentThread();
        tccl = thread.getContextClassLoader();
        filteringClassloader = new FilteringClassLoader(Collections.<String>emptyList(), "com.hazelcast");
        thread.setContextClassLoader(filteringClassloader);
    }

    /**
     * Cleanup usage of separate classloader in test involving clock offset.
     */
    private void cleanupClockOffsetTest() {
        if (tccl != null) {
            currentThread().setContextClassLoader(tccl);
        }
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
    }

    protected Collection<Member> subsetOfMembers(int count) {
        return Arrays.asList(Arrays.copyOfRange(members, 0, count));
    }

    private Member[] members(int clusterSize)
            throws UnknownHostException {
        Member[] members = new Member[clusterSize];
        for (int i = 0; i < members.length; i++) {
            Address nextAddress = new Address("127.0.0.1", 5701 + i);
            members[i] = new MemberImpl(nextAddress, MemberVersion.UNKNOWN, false);
        }
        return members;
    }
}
