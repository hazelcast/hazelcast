/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void put_quorum() {
        cache(0).put(1, "");
    }

    @Test(expected = QuorumException.class)
    public void put_noQuorum() {
        cache(3).put(1, "");
    }

    @Test
    public void getAndPut_quorum() {
        cache(0).getAndPut(1, "");
    }

    @Test(expected = QuorumException.class)
    public void getAndPut_noQuorum() {
        cache(3).getAndPut(1, "");
    }

    @Test
    public void getAndRemove_quorum() {
        cache(0).getAndRemove(1);
    }

    @Test(expected = QuorumException.class)
    public void getAndRemove_noQuorum() {
        cache(3).getAndRemove(1);
    }

    @Test
    public void getAndReplace_quorum() {
        cache(0).getAndReplace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void getAndReplace_noQuorum() {
        cache(3).getAndReplace(1, "");
    }

    @Test
    public void clear_quorum() {
        cache(0).clear();
    }

    @Test(expected = QuorumException.class)
    public void clear_noQuorum() {
        cache(3).clear();
    }

    @Test
    public void putIfAbsent_quorum() {
        cache(0).putIfAbsent(1, "");
    }

    @Test(expected = QuorumException.class)
    public void putIfAbsent_noQuorum() {
        cache(3).putIfAbsent(1, "");
    }

    @Test
    public void putAll_quorum() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache(0).putAll(hashMap);
    }

    @Test(expected = QuorumException.class)
    public void putAll_noQuorum() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache(3).putAll(hashMap);
    }

    @Test
    public void remove_quorum() {
        cache(0).remove(1);
    }

    @Test(expected = QuorumException.class)
    public void remove_noQuorum() {
        cache(3).remove(1);
    }

    @Test
    public void removeAll_quorum() {
        cache(0).removeAll();
    }

    @Test(expected = QuorumException.class)
    public void removeAll_noQuorum() {
        cache(3).removeAll();
    }

    @Test
    public void replace_quorum() {
        cache(0).replace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void replace_noQuorum() {
        cache(3).replace(1, "");
    }

    @Test
    public void getAndPutAsync_quorum() throws Exception {
        cache(0).getAndPutAsync(1, "").get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndPutAsync_noQuorum() throws Exception {
        cache(3).getAndPutAsync(1, "").get();
    }

    @Test
    public void getAndRemoveAsync_quorum() throws Exception {
        cache(0).getAndRemoveAsync(1).get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndRemoveAsync_noQuorum() throws Exception {
        cache(3).getAndRemoveAsync(1).get();
    }

    @Test
    public void getAndReplaceAsync_quorum() throws Exception {
        cache(0).getAndReplaceAsync(1, "").get();
    }

    @Test(expected = ExecutionException.class)
    public void getAndReplaceAsync_noQuorum() throws Exception {
        cache(3).getAndReplaceAsync(1, "").get();
    }

    @Test
    public void invoke_quorum() {
        cache(0).invoke(123, new SimpleEntryProcessor());
    }

    @Test(expected = EntryProcessorException.class)
    public void invoke_noQuorum() {
        cache(3).invoke(123, new SimpleEntryProcessor());
    }

    @Test
    public void invokeAll_quorum() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        EntryProcessorResult epr = cache(0).invokeAll(hashSet, new SimpleEntryProcessor()).get(123);
        assertNull(epr);
    }

    @Test(expected = EntryProcessorException.class)
    public void invokeAll_noQuorum() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(3).invokeAll(hashSet, new SimpleEntryProcessor()).get(123).get();
    }

    @Test
    public void putAsync_quorum() throws Exception {
        cache(0).putAsync(1, "").get();
    }

    @Test(expected = ExecutionException.class)
    public void putAsync_noQuorum() throws Exception {
        cache(3).putAsync(1, "").get();
    }

    @Test
    public void putIfAbsentAsync_quorum() throws Exception {
        cache(0).putIfAbsentAsync(1, "").get();
    }

    @Test(expected = ExecutionException.class)
    public void putIfAbsentAsync_noQuorum() throws Exception {
        cache(3).putIfAbsentAsync(1, "").get();
    }

    @Test
    public void removeAsync_quorum() throws Exception {
        cache(0).removeAsync(1).get();
    }

    @Test(expected = ExecutionException.class)
    public void removeAsync_noQuorum() throws Exception {
        cache(3).removeAsync(1).get();
    }

    @Test
    public void replaceAsync_quorum() throws Exception {
        cache(0).replaceAsync(1, "").get();
    }

    @Test(expected = ExecutionException.class)
    public void replaceAsync_noQuorum() throws Exception {
        cache(3).replaceAsync(1, "").get();
    }

    @Test
    public void putGetWhenQuorumSizeMet() {
        cache(0).put(123, "foo");
        assertEquals("foo", cache(1).get(123));
    }

    @Test
    public void putRemoveGetShouldReturnNullWhenQuorumSizeMet() {
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
        return cache(index, quorumType);
    }
}
