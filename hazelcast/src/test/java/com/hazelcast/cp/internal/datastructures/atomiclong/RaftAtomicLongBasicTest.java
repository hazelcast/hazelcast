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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private IAtomicLong atomicLong;
    private String name = "long1@group1";

    @Before
    public void setup() {
        instances = createInstances();
        atomicLong = createAtomicLong(name);
        assertNotNull(atomicLong);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    protected IAtomicLong createAtomicLong(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getAtomicLong(name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getAtomicLong("long1@metadata");
    }

    @Test
    public void testSet() {
        atomicLong.set(271);
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGet() {
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testDecrementAndGet() {
        assertEquals(-1, atomicLong.decrementAndGet());
        assertEquals(-2, atomicLong.decrementAndGet());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndSet() {
        assertEquals(0, atomicLong.getAndSet(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testAddAndGet() {
        assertEquals(271, atomicLong.addAndGet(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, atomicLong.getAndAdd(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenSuccess() {
        assertTrue(atomicLong.compareAndSet(0, 271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenNotSuccess() {
        assertFalse(atomicLong.compareAndSet(172, 0));
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testAlter() {
        atomicLong.set(2);

        atomicLong.alter(new MultiplyByTwo());

        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGet() {
        atomicLong.set(2);

        long result = atomicLong.alterAndGet(new MultiplyByTwo());

        assertEquals(4, result);
    }

    @Test
    public void testGetAndAlter() {
        atomicLong.set(2);

        long result = atomicLong.getAndAlter(new MultiplyByTwo());

        assertEquals(2, result);
        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Void> f = atomicLong.alterAsync(new MultiplyByTwo());
        f.get();

        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGetAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.alterAndGetAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(4, result);
    }

    @Test
    public void testGetAndAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.getAndAlterAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(2, result);
        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testApply() {
        atomicLong.set(2);

        long result = atomicLong.apply(new MultiplyByTwo());

        assertEquals(4, result);
        assertEquals(2, atomicLong.get());
    }

    @Test
    public void testApplyAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        Future<Long> f = atomicLong.applyAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(4, result);
        assertEquals(2, atomicLong.get());
    }

    @Test
    public void testLocalGet_withLeaderLocalPolicy() {
        atomicLong.set(3);

        RaftAtomicLongProxy atomicLongProxy = (RaftAtomicLongProxy) atomicLong;
        long v = atomicLongProxy.localGet(QueryPolicy.LEADER_LOCAL);

        assertEquals(3, v);
    }

    @Test
    public void testLocalGet_withAnyLocalPolicy() {
        atomicLong.set(3);

        // I may not be the leader...
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftAtomicLongProxy atomicLongProxy = (RaftAtomicLongProxy) atomicLong;
                final long v = atomicLongProxy.localGet(QueryPolicy.ANY_LOCAL);
                assertEquals(3, v);
            }
        });
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicLong atomicLong = createAtomicLong(randomName());
        assertEquals(DEFAULT_GROUP_NAME, getGroupId(atomicLong).name());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        atomicLong.destroy();
        atomicLong.incrementAndGet();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        atomicLong.destroy();

        atomicLong = createAtomicLong(name);
        atomicLong.incrementAndGet();
    }

    @Test
    public void testMultipleDestroy() {
        atomicLong.destroy();
        atomicLong.destroy();
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicLong.destroy();

        final CPGroupId groupId = getGroupId(atomicLong);
        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                CPGroup group = invocationManager.<CPGroup>invoke(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId)).join();
                assertEquals(CPGroupStatus.DESTROYED, group.status());
            }
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

    protected CPGroupId getGroupId(IAtomicLong atomicLong) {
        return ((RaftAtomicLongProxy) atomicLong).getGroupId();
    }

    public static class MultiplyByTwo implements IFunction<Long, Long> {

        @Override
        public Long apply(Long input) {
            return input * 2;
        }
    }
}
