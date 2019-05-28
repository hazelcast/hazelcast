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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetCountOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftCountDownLatchSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Integer> {

    private ICountDownLatch latch;
    private String name = "latch";

    @Before
    public void createProxy() {
        latch = getCPSubsystem().getCountDownLatch(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((RaftCountDownLatchProxy) latch).getGroupId();
    }

    @Override
    protected Integer setAndGetInitialValue() {
        assertTrue(latch.trySetCount(5));
        latch.countDown();
        return latch.getCount();
    }

    @Override
    protected Integer readValue() {
        return latch.getCount();
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new GetCountOp(name);
    }
}
