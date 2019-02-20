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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Long> {

    protected IAtomicLong atomicLong;

    @Before
    public void setup() {
        HazelcastInstance[] instances = createInstances();
        String name = "long@group";
        atomicLong = createAtomicLong(instances, name);
    }

    protected IAtomicLong createAtomicLong(HazelcastInstance[] instances, String name) {
        HazelcastInstance apInstance = instances[instances.length - 1];
        return apInstance.getCPSubsystem().getAtomicLong(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((RaftAtomicLongProxy) atomicLong).getGroupId();
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
}
