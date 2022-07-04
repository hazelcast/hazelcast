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

package com.hazelcast.splitbrainprotection.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void put_splitBrainProtection() {
        cache(0).put(1, "");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void put_noSplitBrainProtection() {
        cache(3).put(1, "");
    }

    @Test
    public void getAndPut_splitBrainProtection() {
        cache(0).getAndPut(1, "");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndPut_noSplitBrainProtection() {
        cache(3).getAndPut(1, "");
    }

    @Test
    public void getAndRemove_splitBrainProtection() {
        cache(0).getAndRemove(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndRemove_noSplitBrainProtection() {
        cache(3).getAndRemove(1);
    }

    @Test
    public void getAndReplace_splitBrainProtection() {
        cache(0).getAndReplace(1, "");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndReplace_noSplitBrainProtection() {
        cache(3).getAndReplace(1, "");
    }

    @Test
    public void clear_splitBrainProtection() {
        cache(0).clear();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void clear_noSplitBrainProtection() {
        cache(3).clear();
    }

    @Test
    public void putIfAbsent_splitBrainProtection() {
        cache(0).putIfAbsent(1, "");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void putIfAbsent_noSplitBrainProtection() {
        cache(3).putIfAbsent(1, "");
    }

    @Test
    public void putAll_splitBrainProtection() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache(0).putAll(hashMap);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void putAll_noSplitBrainProtection() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache(3).putAll(hashMap);
    }

    @Test
    public void remove_splitBrainProtection() {
        cache(0).remove(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void remove_noSplitBrainProtection() {
        cache(3).remove(1);
    }

    @Test
    public void removeAll_splitBrainProtection() {
        cache(0).removeAll();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void removeAll_noSplitBrainProtection() {
        cache(3).removeAll();
    }

    @Test
    public void replace_splitBrainProtection() {
        cache(0).replace(1, "");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void replace_noSplitBrainProtection() {
        cache(3).replace(1, "");
    }

    @Test
    public void getAndPutAsync_splitBrainProtection() throws Exception {
        cache(0).getAndPutAsync(1, "").toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndPutAsync_noSplitBrainProtection() throws Exception {
        cache(3).getAndPutAsync(1, "").toCompletableFuture().get();
    }

    @Test
    public void getAndRemoveAsync_splitBrainProtection() throws Exception {
        cache(0).getAndRemoveAsync(1).toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndRemoveAsync_noSplitBrainProtection() throws Exception {
        cache(3).getAndRemoveAsync(1).toCompletableFuture().get();
    }

    @Test
    public void getAndReplaceAsync_splitBrainProtection() throws Exception {
        cache(0).getAndReplaceAsync(1, "").toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndReplaceAsync_noSplitBrainProtection() throws Exception {
        cache(3).getAndReplaceAsync(1, "").toCompletableFuture().get();
    }

    @Test
    public void invoke_splitBrainProtection() {
        cache(0).invoke(123, new SimpleEntryProcessor());
    }

    @Test(expected = EntryProcessorException.class)
    public void invoke_noSplitBrainProtection() {
        cache(3).invoke(123, new SimpleEntryProcessor());
    }

    @Test
    public void invokeAll_splitBrainProtection() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        EntryProcessorResult epr = cache(0).invokeAll(hashSet, new SimpleEntryProcessor()).get(123);
        assertNull(epr);
    }

    @Test(expected = EntryProcessorException.class)
    public void invokeAll_noSplitBrainProtection() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(3).invokeAll(hashSet, new SimpleEntryProcessor()).get(123).get();
    }

    @Test
    public void putAsync_splitBrainProtection() throws Exception {
        cache(0).putAsync(1, "").toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void putAsync_noSplitBrainProtection() throws Exception {
        cache(3).putAsync(1, "").toCompletableFuture().get();
    }

    @Test
    public void putIfAbsentAsync_splitBrainProtection() throws Exception {
        cache(0).putIfAbsentAsync(1, "").toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void putIfAbsentAsync_noSplitBrainProtection() throws Exception {
        cache(3).putIfAbsentAsync(1, "").toCompletableFuture().get();
    }

    @Test
    public void removeAsync_splitBrainProtection() throws Exception {
        cache(0).removeAsync(1).toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void removeAsync_noSplitBrainProtection() throws Exception {
        cache(3).removeAsync(1).toCompletableFuture().get();
    }

    @Test
    public void replaceAsync_splitBrainProtection() throws Exception {
        cache(0).replaceAsync(1, "").toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void replaceAsync_noSplitBrainProtection() throws Exception {
        cache(3).replaceAsync(1, "").toCompletableFuture().get();
    }

    @Test
    public void putGetwhenSplitBrainProtectionSizeMet() {
        cache(0).put(123, "foo");
        assertEquals("foo", cache(1).get(123));
    }

    @Test
    public void putRemoveGetShouldReturnNullwhenSplitBrainProtectionSizeMet() {
        cache(0).put(123, "foo");
        cache(0).remove(123);
        assertNull(cache(1).get(123));
    }

    public static class SimpleEntryProcessor implements EntryProcessor<Integer, String, Void>, Serializable {
        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
            entry.setValue("Foo");
            return null;
        }
    }

    protected ICache<Integer, String> cache(int index) {
        return cache(index, splitBrainProtectionOn);
    }
}
