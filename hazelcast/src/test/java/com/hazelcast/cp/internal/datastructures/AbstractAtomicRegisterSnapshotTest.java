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

package com.hazelcast.cp.internal.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractAtomicRegisterSnapshotTest<T> extends HazelcastRaftTestSupport {

    private static final int SNAPSHOT_THRESHOLD = 100;

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = newInstances(3);
    }

    protected CPSubsystem getCPSubsystem() {
        return instances[0].getCPSubsystem();
    }

    protected abstract CPGroupId getGroupId();

    protected abstract T setAndGetInitialValue();

    protected abstract T readValue();

    protected abstract RaftOp getQueryRaftOp();

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(SNAPSHOT_THRESHOLD);
        return config;
    }

    @Test
    public void test_snapshot() throws Exception {
        T initialValue = setAndGetInitialValue();

        // force snapshot
        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            T v = readValue();
            assertEquals(initialValue, v);
        }

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion.
        assertTrueEventually(() -> {
            InternalCompletableFuture<Object> future = queryLocally(instance);
            try {
                T value = getValue(future);
                assertEquals(initialValue, value);
            } catch (CPSubsystemException e) {
                // Raft node may not be created yet...
                throw new AssertionError(e);
            }
        });

        assertTrueAllTheTime(() -> {
            InternalCompletableFuture<Object> future = queryLocally(instance);
            T value = getValue(future);
            assertEquals(initialValue, value);
        }, 5);
    }

    protected T getValue(InternalCompletableFuture<Object> future) {
        return (T) future.joinInternal();
    }

    private InternalCompletableFuture<Object> queryLocally(HazelcastInstance instance) {
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        return invocationManager.queryLocally(getGroupId(), getQueryRaftOp(), QueryPolicy.ANY_LOCAL);
    }

}
