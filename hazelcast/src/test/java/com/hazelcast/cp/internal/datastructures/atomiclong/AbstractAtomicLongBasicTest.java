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
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractAtomicLongBasicTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    protected String name;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        atomicLong = createAtomicLong(name);
        assertNotNull(atomicLong);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract IAtomicLong createAtomicLong(String name);

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getAtomicLong("long@" + CPGroup.METADATA_CP_GROUP_NAME);
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
    public void testGetAndDecrement() {
        assertEquals(0, atomicLong.getAndDecrement());
        assertEquals(-1, atomicLong.getAndDecrement());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndIncrement() {
        assertEquals(0, atomicLong.getAndIncrement());
        assertEquals(1, atomicLong.getAndIncrement());
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

        CompletionStage<Void> f = atomicLong.alterAsync(new MultiplyByTwo());
        f.toCompletableFuture().get();

        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGetAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        CompletionStage<Long> f = atomicLong.alterAndGetAsync(new MultiplyByTwo());
        long result = f.toCompletableFuture().get();

        assertEquals(4, result);
    }

    @Test
    public void testGetAndAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        CompletionStage<Long> f = atomicLong.getAndAlterAsync(new MultiplyByTwo());
        long result = f.toCompletableFuture().get();

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

        CompletionStage<Long> f = atomicLong.applyAsync(new MultiplyByTwo());
        long result = f.toCompletableFuture().get();

        assertEquals(4, result);
        assertEquals(2, atomicLong.get());
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

    protected CPGroupId getGroupId(IAtomicLong atomicLong) {
        return ((AtomicLongProxy) atomicLong).getGroupId();
    }

    public static class MultiplyByTwo implements IFunction<Long, Long> {

        @Override
        public Long apply(Long input) {
            return input * 2;
        }
    }
}
