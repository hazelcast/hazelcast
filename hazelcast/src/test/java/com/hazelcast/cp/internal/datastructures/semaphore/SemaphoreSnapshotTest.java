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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Integer> {

    private ISemaphore semaphore;
    private String name = "semaphore";


    @Before
    public void createProxy() {
        semaphore = getCPSubsystem().getSemaphore(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((SessionAwareSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    protected Integer setAndGetInitialValue() {
        assertTrue(semaphore.init(5));
        assertTrue(semaphore.tryAcquire());
        return semaphore.availablePermits();
    }

    @Override
    protected Integer readValue() {
        return semaphore.availablePermits();
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new AvailablePermitsOp(name);
    }
}
