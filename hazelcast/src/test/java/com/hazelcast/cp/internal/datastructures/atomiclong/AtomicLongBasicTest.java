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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongBasicTest extends AbstractAtomicLongBasicTest {

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    @Override
    protected String getName() {
        return "long@group";
    }

    @Override
    protected IAtomicLong createAtomicLong(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getAtomicLong(name);
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicLong atomicLong = createAtomicLong(randomName());
        assertEquals(DEFAULT_GROUP_NAME, getGroupId(atomicLong).getName());
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicLong.destroy();

        CPGroupId groupId = getGroupId(atomicLong);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            CPGroup group = invocationManager.<CPGroup>query(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId), LINEARIZABLE).join();
            assertEquals(CPGroupStatus.DESTROYED, group.status());
        });

        try {
            atomicLong.incrementAndGet();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        atomicLong = createAtomicLong(name);
        CPGroupId newGroupId = getGroupId(atomicLong);
        assertNotEquals(groupId, newGroupId);

        atomicLong.incrementAndGet();
    }
}
