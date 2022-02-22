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
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.AtomicRefProxy;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractAtomicRefBasicTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicReference<String> atomicRef;
    protected String name;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        atomicRef = createAtomicRef(name);
        assertNotNull(atomicRef);
    }

    protected abstract String getName();

    protected abstract HazelcastInstance[] createInstances();

    protected abstract IAtomicReference<String> createAtomicRef(String name);


    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getAtomicReference("ref@" + CPGroup.METADATA_CP_GROUP_NAME);
    }

    @Test
    public void test_compareAndSet() {
        assertTrue(atomicRef.compareAndSet(null, "str1"));
        assertEquals("str1", atomicRef.get());
        assertFalse(atomicRef.compareAndSet(null, "str1"));
        assertTrue(atomicRef.compareAndSet("str1", "str2"));
        assertEquals("str2", atomicRef.get());
        assertFalse(atomicRef.compareAndSet("str1", "str2"));
        assertTrue(atomicRef.compareAndSet("str2", null));
        assertNull(atomicRef.get());
        assertFalse(atomicRef.compareAndSet("str2", null));
    }

    @Test
    public void test_compareAndSetAsync() throws ExecutionException, InterruptedException {
        assertTrue(atomicRef.compareAndSetAsync(null, "str1").toCompletableFuture().get());
        assertEquals("str1", atomicRef.getAsync().toCompletableFuture().get());
        assertFalse(atomicRef.compareAndSetAsync(null, "str1").toCompletableFuture().get());
        assertTrue(atomicRef.compareAndSetAsync("str1", "str2").toCompletableFuture().get());
        assertEquals("str2", atomicRef.getAsync().toCompletableFuture().get());
        assertFalse(atomicRef.compareAndSetAsync("str1", "str2").toCompletableFuture().get());
        assertTrue(atomicRef.compareAndSetAsync("str2", null).toCompletableFuture().get());
        assertNull(atomicRef.getAsync().toCompletableFuture().get());
        assertFalse(atomicRef.compareAndSetAsync("str2", null).toCompletableFuture().get());
    }

    @Test
    public void test_set() {
        atomicRef.set("str1");
        assertEquals("str1", atomicRef.get());
        assertEquals("str1", atomicRef.getAndSet("str2"));
        assertEquals("str2", atomicRef.get());
    }

    @Test
    public void test_get_whenRefIsException() {
        IAtomicReference atomicRef = createAtomicRef(randomName());
        RuntimeException value = new RuntimeException("boom");
        atomicRef.set(value);
        assertInstanceOf(RuntimeException.class, atomicRef.get());
        assertEquals(value.getMessage(),
                ((RuntimeException) atomicRef.get()).getMessage());
    }

    @Test
    public void test_getAsync_whenRefIsException()
            throws ExecutionException, InterruptedException {
        IAtomicReference atomicRef = createAtomicRef(randomName());
        RuntimeException value = new RuntimeException("boom");
        atomicRef.set(value);
        assertInstanceOf(RuntimeException.class,
                atomicRef.getAsync().toCompletableFuture().get());
        assertEquals(value.getMessage(),
                ((RuntimeException) atomicRef.getAsync().toCompletableFuture().get()).getMessage());
    }

    @Test
    public void test_getAndSet() {
        assertNull(atomicRef.getAndSet("str1"));
        assertEquals("str1", atomicRef.getAndSet("str2"));
        assertEquals("str2", atomicRef.get());
    }

    @Test
    public void test_setAsync() throws ExecutionException, InterruptedException {
        atomicRef.setAsync("str1").toCompletableFuture().get();
        assertEquals("str1", atomicRef.get());
        assertEquals("str1", atomicRef.getAndSetAsync("str2").toCompletableFuture().get());
        assertEquals("str2", atomicRef.get());
    }

    @Test
    public void test_isNull() throws ExecutionException, InterruptedException {
        assertTrue(atomicRef.isNull());
        assertTrue(atomicRef.isNullAsync().toCompletableFuture().get());

        atomicRef.set("str1");

        assertFalse(atomicRef.isNull());
        assertFalse(atomicRef.isNullAsync().toCompletableFuture().get());
    }

    @Test
    public void test_clear() {
        atomicRef.set("str1");
        atomicRef.clear();

        assertTrue(atomicRef.isNull());
    }

    @Test
    public void test_clearAsync() throws ExecutionException, InterruptedException {
        atomicRef.set("str1");
        atomicRef.clearAsync().toCompletableFuture().get();

        assertTrue(atomicRef.isNull());
    }

    @Test
    public void test_contains() throws ExecutionException, InterruptedException {
        assertTrue(atomicRef.contains(null));
        assertTrue(atomicRef.containsAsync(null).toCompletableFuture().get());
        assertFalse(atomicRef.contains("str1"));
        assertFalse(atomicRef.containsAsync("str1").toCompletableFuture().get());

        atomicRef.set("str1");

        assertFalse(atomicRef.contains(null));
        assertFalse(atomicRef.containsAsync(null).toCompletableFuture().get());
        assertTrue(atomicRef.contains("str1"));
        assertTrue(atomicRef.containsAsync("str1").toCompletableFuture().get());
    }

    @Test
    public void test_alter() {
        atomicRef.set("str1");

        atomicRef.alter(new AppendStringFunction("str2"));

        String val = atomicRef.get();
        assertEquals("str1 str2", val);

        val = atomicRef.alterAndGet(new AppendStringFunction("str3"));
        assertEquals("str1 str2 str3", val);

        val = atomicRef.getAndAlter(new AppendStringFunction("str4"));
        assertEquals("str1 str2 str3", val);
        assertEquals("str1 str2 str3 str4", atomicRef.get());
    }

    @Test
    public void test_alterAsync() throws ExecutionException, InterruptedException {
        atomicRef.set("str1");

        atomicRef.alterAsync(new AppendStringFunction("str2")).toCompletableFuture().get();

        String val = atomicRef.get();
        assertEquals("str1 str2", val);

        val = atomicRef.alterAndGetAsync(new AppendStringFunction("str3")).toCompletableFuture().get();
        assertEquals("str1 str2 str3", val);

        val = atomicRef.getAndAlterAsync(new AppendStringFunction("str4")).toCompletableFuture().get();
        assertEquals("str1 str2 str3", val);
        assertEquals("str1 str2 str3 str4", atomicRef.get());
    }

    @Test
    public void test_apply() throws ExecutionException, InterruptedException {
        atomicRef.set("str1");

        String val = atomicRef.apply(new AppendStringFunction("str2"));
        assertEquals("str1 str2", val);
        assertEquals("str1", atomicRef.get());

        val = atomicRef.applyAsync(new AppendStringFunction("str2")).toCompletableFuture().get();
        assertEquals("str1 str2", val);
        assertEquals("str1", atomicRef.get());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        atomicRef.destroy();
        atomicRef.set("str1");
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        atomicRef.destroy();

        atomicRef = createAtomicRef(name);
        atomicRef.set("str1");
    }

    @Test
    public void testMultipleDestroy() {
        atomicRef.destroy();
        atomicRef.destroy();
    }

    protected CPGroupId getGroupId(IAtomicReference ref) {
        return ((AtomicRefProxy) ref).getGroupId();
    }

    public static class AppendStringFunction implements IFunction<String, String> {

        private String suffix;

        AppendStringFunction(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String apply(String input) {
            return input + " " + suffix;
        }
    }
}
