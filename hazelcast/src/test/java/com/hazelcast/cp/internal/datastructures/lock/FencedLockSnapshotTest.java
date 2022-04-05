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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Long> {

    private FencedLock lock;
    private String name = "lock";

    @Before
    public void createProxy() {
        lock = getCPSubsystem().getLock(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return lock.getGroupId();
    }

    @Override
    protected Long setAndGetInitialValue() {
        return lock.lockAndGetFence();
    }

    @Override
    protected Long readValue() {
        return lock.getFence();
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new GetLockOwnershipStateOp(name);
    }

    @Override
    protected Long getValue(InternalCompletableFuture<Object> future) {
        LockOwnershipState state = (LockOwnershipState) future.joinInternal();
        return state.getFence();
    }
}
