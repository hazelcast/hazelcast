/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheReadWriteQuorumTest extends HazelcastTestSupport {

    private static final String CACHE_NAME_PREFIX = "cacheQuorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";

    private static HazelcastServerCachingProvider cachingProvider1;
    private static HazelcastServerCachingProvider cachingProvider2;
    private static HazelcastServerCachingProvider cachingProvider3;
    private static HazelcastServerCachingProvider cachingProvider4;
    private static HazelcastServerCachingProvider cachingProvider5;

    private ICache<Integer, String> cache1;
    private ICache<Integer, String> cache2;
    private ICache<Integer, String> cache3;
    private ICache<Integer, String> cache4;
    private ICache<Integer, String> cache5;

    @BeforeClass
    public static void initialize() throws Exception {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(CACHE_NAME_PREFIX + "*");
        cacheConfig.setQuorumName(QUORUM_ID);

        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setType(QuorumType.READ_WRITE);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);

        PartitionedCluster cluster = new PartitionedCluster(new TestHazelcastInstanceFactory())
                .partitionFiveMembersThreeAndTwo(cacheConfig, quorumConfig);
        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(cluster.h1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(cluster.h2);
        cachingProvider3 = HazelcastServerCachingProvider.createCachingProvider(cluster.h3);
        cachingProvider4 = HazelcastServerCachingProvider.createCachingProvider(cluster.h4);
        cachingProvider5 = HazelcastServerCachingProvider.createCachingProvider(cluster.h5);
    }

    @Before
    public void setUp() {
        String cacheName = CACHE_NAME_PREFIX + randomString();
        cache1 = (ICache<Integer, String>) cachingProvider1.getCacheManager().<Integer, String>getCache(cacheName);
        cache2 = (ICache<Integer, String>) cachingProvider2.getCacheManager().<Integer, String>getCache(cacheName);
        cache3 = (ICache<Integer, String>) cachingProvider3.getCacheManager().<Integer, String>getCache(cacheName);
        cache4 = (ICache<Integer, String>) cachingProvider4.getCacheManager().<Integer, String>getCache(cacheName);
        cache5 = (ICache<Integer, String>) cachingProvider5.getCacheManager().<Integer, String>getCache(cacheName);
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testGetOperationSuccessfulWhenQuorumSizeMet() {
        cache1.get(1);
    }

    @Test(expected = QuorumException.class)
    public void testGetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.get(1);
    }

    @Test
    public void testPutOperationSuccessfulWhenQuorumSizeMet() {
        cache1.put(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.put(1, "");
    }

    @Test
    public void testContainsOperationSuccessfulWhenQuorumSizeMet() {
        cache1.containsKey(1);
    }

    @Test(expected = QuorumException.class)
    public void testContainsOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.containsKey(1);
    }

    @Test
    public void testGetAllOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache1.getAll(hashSet);
    }

    @Test(expected = QuorumException.class)
    public void testGetAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache4.getAll(hashSet);
    }

    @Test
    public void testGetAndPutOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndPut(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testGetAndPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndPut(1, "");
    }

    @Test
    public void testGetAndRemoveOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndRemove(1);
    }

    @Test(expected = QuorumException.class)
    public void testGetAndRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndRemove(1);
    }

    @Test
    public void testGetAndReplaceOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndReplace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testGetAndReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndReplace(1, "");
    }

    @Test
    public void testClearOperationSuccessfulWhenQuorumSizeMet() {
        cache1.clear();
    }

    @Test(expected = QuorumException.class)
    public void testClearOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.clear();
    }

    @Test
    public void testIteratorOperationSuccessfulWhenQuorumSizeMet() {
        cache1.iterator();
    }

    @Test(expected = QuorumException.class)
    public void testIteratorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.iterator();
    }

    @Test
    public void testPutIfAbsentOperationSuccessfulWhenQuorumSizeMet() {
        cache1.putIfAbsent(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testPutIfAbsentOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.putIfAbsent(1, "");
    }

    @Test
    public void testPutAllOperationSuccessfulWhenQuorumSizeMet() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache1.putAll(hashMap);
    }

    @Test(expected = QuorumException.class)
    public void testPutAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache4.putAll(hashMap);
    }

    @Test
    public void testRemoveOperationSuccessfulWhenQuorumSizeMet() {
        cache1.remove(1);
    }

    @Test(expected = QuorumException.class)
    public void testRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.remove(1);
    }

    @Test
    public void testRemoveAllOperationSuccessfulWhenQuorumSizeMet() {
        cache1.removeAll();
    }

    @Test(expected = QuorumException.class)
    public void testRemoveAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.removeAll();
    }

    @Test
    public void testReplaceOperationSuccessfulWhenQuorumSizeMet() {
        cache1.replace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.replace(1, "");
    }

    @Test
    public void testGetAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAsync(1);
        foo.get();
    }

    @Test
    public void testGetAndPutAsyncOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndPutAsync(1, "");
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndPutAsync(1, "");
        foo.get();
    }

    @Test
    public void testGetAndRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAndRemoveAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndRemoveAsync(1);
        foo.get();
    }

    @Test
    public void testGetAndReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAndReplaceAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndReplaceAsync(1, "");
        foo.get();
    }

    @Test
    public void testPutAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Void> foo = cache1.putAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Void> foo = cache4.putAsync(1, "");
        foo.get();
    }

    @Test
    public void testPutIfAbsentAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.putIfAbsentAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutIfAbsentAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.putIfAbsentAsync(1, "");
        foo.get();
    }

    @Test
    public void testRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.removeAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.removeAsync(1);
        foo.get();
    }

    @Test
    public void testReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.replaceAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.replaceAsync(1, "");
        foo.get();
    }

    @Test
    public void testInvokeOperationSuccessfulWhenQuorumSizeMet() {
        cache1.invoke(123, new SimpleEntryProcessor());
    }

    @Test(expected = EntryProcessorException.class)
    public void testInvokeOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.invoke(123, new SimpleEntryProcessor());
    }

    @Test
    public void testInvokeAllOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        EntryProcessorResult epr = cache1.invokeAll(hashSet, new SimpleEntryProcessor()).get(123);
        assertNull(epr);
    }

    @Test(expected = EntryProcessorException.class)
    public void testInvokeAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache4.invokeAll(hashSet, new SimpleEntryProcessor()).get(123).get();
    }

    @Test
    public void testPutGetWhenQuorumSizeMet() {
        cache1.put(123, "foo");
        assertEquals("foo", cache2.get(123));
    }

    @Test
    public void testPutRemoveGetShouldReturnNullWhenQuorumSizeMet() {
        cache1.put(123, "foo");
        cache1.remove(123);
        assertNull(cache2.get(123));
    }

    public static class SimpleEntryProcessor implements EntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
            entry.setValue("Foo");
            return null;
        }
    }
}
