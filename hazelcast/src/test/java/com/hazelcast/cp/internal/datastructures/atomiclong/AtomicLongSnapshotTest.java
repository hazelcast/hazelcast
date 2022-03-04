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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.LocalGetOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Long> {

    private IAtomicLong atomicLong;
    private String name = "long";

    @Before
    public void createProxy() {
        atomicLong = getCPSubsystem().getAtomicLong(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((AtomicLongProxy) atomicLong).getGroupId();
    }

    @Override
    protected Long setAndGetInitialValue() {
        long value = 13131L;
        atomicLong.set(value);
        return value;
    }

    @Override
    protected Long readValue() {
        return atomicLong.get();
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new LocalGetOp(name);
    }
}
