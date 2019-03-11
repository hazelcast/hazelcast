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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.RaftAtomicRefProxy;
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

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicRefBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private IAtomicReference<String> atomicRef;
    private String name = "ref@group1";

    @Before
    public void setup() {
        instances = createInstances();
        atomicRef = createAtomicRef(name);
        assertNotNull(atomicRef);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    protected <T> IAtomicReference<T> createAtomicRef(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getAtomicReference(name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getAtomicReference("ref@metadata");
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
        assertTrue(atomicRef.compareAndSetAsync(null, "str1").get());
        assertEquals("str1", atomicRef.getAsync().get());
        assertFalse(atomicRef.compareAndSetAsync(null, "str1").get());
        assertTrue(atomicRef.compareAndSetAsync("str1", "str2").get());
        assertEquals("str2", atomicRef.getAsync().get());
        assertFalse(atomicRef.compareAndSetAsync("str1", "str2").get());
        assertTrue(atomicRef.compareAndSetAsync("str2", null).get());
        assertNull(atomicRef.getAsync().get());
        assertFalse(atomicRef.compareAndSetAsync("str2", null).get());
    }

    @Test
    public void test_set() {
        atomicRef.set("str1");
        assertEquals("str1", atomicRef.get());
        assertEquals("str1", atomicRef.getAndSet("str2"));
        assertEquals("str2", atomicRef.get());
    }

    @Test
    public void test_setAsync() throws ExecutionException, InterruptedException {
        atomicRef.setAsync("str1").get();
        assertEquals("str1", atomicRef.get());
        assertEquals("str1", atomicRef.getAndSetAsync("str2").get());
        assertEquals("str2", atomicRef.get());
    }

    @Test
    public void test_isNull() throws ExecutionException, InterruptedException {
        assertTrue(atomicRef.isNull());
        assertTrue(atomicRef.isNullAsync().get());

        atomicRef.set("str1");

        assertFalse(atomicRef.isNull());
        assertFalse(atomicRef.isNullAsync().get());
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
        atomicRef.clearAsync().get();

        assertTrue(atomicRef.isNull());
    }

    @Test
    public void test_contains() throws ExecutionException, InterruptedException {
        assertTrue(atomicRef.contains(null));
        assertTrue(atomicRef.containsAsync(null).get());
        assertFalse(atomicRef.contains("str1"));
        assertFalse(atomicRef.containsAsync("str1").get());

        atomicRef.set("str1");

        assertFalse(atomicRef.contains(null));
        assertFalse(atomicRef.containsAsync(null).get());
        assertTrue(atomicRef.contains("str1"));
        assertTrue(atomicRef.containsAsync("str1").get());
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

        atomicRef.alterAsync(new AppendStringFunction("str2")).get();

        String val = atomicRef.get();
        assertEquals("str1 str2", val);

        val = atomicRef.alterAndGetAsync(new AppendStringFunction("str3")).get();
        assertEquals("str1 str2 str3", val);

        val = atomicRef.getAndAlterAsync(new AppendStringFunction("str4")).get();
        assertEquals("str1 str2 str3", val);
        assertEquals("str1 str2 str3 str4", atomicRef.get());
    }

    @Test
    public void test_apply() throws ExecutionException, InterruptedException {
        atomicRef.set("str1");

        String val = atomicRef.apply(new AppendStringFunction("str2"));
        assertEquals("str1 str2", val);
        assertEquals("str1", atomicRef.get());

        val = atomicRef.applyAsync(new AppendStringFunction("str2")).get();
        assertEquals("str1 str2", val);
        assertEquals("str1", atomicRef.get());
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicReference<String> atomicRef = createAtomicRef(randomName());
        assertEquals(DEFAULT_GROUP_NAME, getGroupId(atomicRef).name());
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

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicRef.destroy();

        final CPGroupId groupId = getGroupId(atomicRef);
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
            atomicRef.get();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        atomicRef = createAtomicRef(name);
        assertNotEquals(groupId, getGroupId(atomicRef));

        atomicRef.set("str1");
    }

    protected CPGroupId getGroupId(IAtomicReference ref) {
        return ((RaftAtomicRefProxy) ref).getGroupId();
    }


    public static class AppendStringFunction implements IFunction<String, String> {

        private String suffix;

        public AppendStringFunction(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String apply(String input) {
            return input + " " + suffix;
        }
    }
}
