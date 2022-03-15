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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicRefBasicTest extends AbstractAtomicRefBasicTest {

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    @Override
    protected String getName() {
        return "ref@group";
    }

    @Override
    protected IAtomicReference<String> createAtomicRef(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getAtomicReference(name);
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicReference<String> atomicRef = createAtomicRef(randomName());
        assertEquals(DEFAULT_GROUP_NAME, getGroupId(atomicRef).getName());
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicRef.destroy();

        CPGroupId groupId = getGroupId(atomicRef);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            CPGroup group = invocationManager.<CPGroup>invoke(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId)).join();
            assertEquals(CPGroupStatus.DESTROYED, group.status());
        });

        try {
            atomicRef.get();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        atomicRef = createAtomicRef(name);
        assertNotEquals(groupId, getGroupId(atomicRef));

        atomicRef.set("str1");
    }
}
