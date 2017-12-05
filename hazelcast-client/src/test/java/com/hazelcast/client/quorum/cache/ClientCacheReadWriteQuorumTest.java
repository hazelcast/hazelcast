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

package com.hazelcast.client.quorum.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
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

import static com.hazelcast.client.quorum.QuorumTestUtil.getClientConfig;
import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheReadWriteQuorumTest extends HazelcastTestSupport {

    private static final String CACHE_NAME_PREFIX = "cacheQuorum";

    private static PartitionedCluster cluster;

    private static HazelcastInstance c1;
    private static HazelcastInstance c2;
    private static HazelcastInstance c3;
    private static HazelcastInstance c4;
    private static HazelcastInstance c5;

    private static ICache<Integer, String> cache1;
    private static ICache<Integer, String> cache2;
    private static ICache<Integer, String> cache3;
    private static ICache<Integer, String> cache4;
    private static ICache<Integer, String> cache5;

    private static TestHazelcastFactory factory;

    @BeforeClass
    public static void initialize() {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setType(QuorumType.READ_WRITE);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);

        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(CACHE_NAME_PREFIX + "*");
        cacheConfig.setQuorumName(QUORUM_ID);
        factory = new TestHazelcastFactory();
        cluster = new PartitionedCluster(factory).createFiveMemberCluster(cacheConfig, quorumConfig);
        initializeClients();
        initializeCaches();
        cluster.splitFiveMembersThreeAndTwo(QUORUM_ID);
        verifyClients();
    }

    private static void initializeClients() {
        c1 = factory.newHazelcastClient(getClientConfig(cluster.instance[0]));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.instance[1]));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.instance[2]));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.instance[3]));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.instance[4]));
    }

    private static void initializeCaches() {
        HazelcastClientCachingProvider cachingProvider1 = HazelcastClientCachingProvider.createCachingProvider(c1);
        HazelcastClientCachingProvider cachingProvider2 = HazelcastClientCachingProvider.createCachingProvider(c2);
        HazelcastClientCachingProvider cachingProvider3 = HazelcastClientCachingProvider.createCachingProvider(c3);
        HazelcastClientCachingProvider cachingProvider4 = HazelcastClientCachingProvider.createCachingProvider(c4);
        HazelcastClientCachingProvider cachingProvider5 = HazelcastClientCachingProvider.createCachingProvider(c5);

        String cacheName = CACHE_NAME_PREFIX + randomString();
        cache1 = (ICache<Integer, String>) cachingProvider1.getCacheManager().<Integer, String>getCache(cacheName);
        cache2 = (ICache<Integer, String>) cachingProvider2.getCacheManager().<Integer, String>getCache(cacheName);
        cache3 = (ICache<Integer, String>) cachingProvider3.getCacheManager().<Integer, String>getCache(cacheName);
        cache4 = (ICache<Integer, String>) cachingProvider4.getCacheManager().<Integer, String>getCache(cacheName);
        cache5 = (ICache<Integer, String>) cachingProvider5.getCacheManager().<Integer, String>getCache(cacheName);
    }

    private static void verifyClients() {
        assertClusterSizeEventually(3, c1, c2, c3);
        assertClusterSizeEventually(2, c4, c5);
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        factory.terminateAll();
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
        Future<String> future = cache1.getAsync(1);
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> future = cache4.getAsync(1);
        future.get();
    }

    @Test
    public void testGetAndPutAsyncOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndPutAsync(1, "");
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> future = cache4.getAndPutAsync(1, "");
        future.get();
    }

    @Test
    public void testGetAndRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> future = cache1.getAndRemoveAsync(1);
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> future = cache4.getAndRemoveAsync(1);
        future.get();
    }

    @Test
    public void testGetAndReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> future = cache1.getAndReplaceAsync(1, "");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> future = cache4.getAndReplaceAsync(1, "");
        future.get();
    }

    @Test
    public void testPutAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Void> future = cache1.putAsync(1, "");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Void> future = cache4.putAsync(1, "");
        future.get();
    }

    @Test
    public void testPutIfAbsentAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> future = cache1.putIfAbsentAsync(1, "");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutIfAbsentAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> future = cache4.putIfAbsentAsync(1, "");
        future.get();
    }

    @Test
    public void testRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> future = cache1.removeAsync(1);
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> future = cache4.removeAsync(1);
        future.get();
    }

    @Test
    public void testReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> future = cache1.replaceAsync(1, "");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> future = cache4.replaceAsync(1, "");
        future.get();
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

    public static class SimpleEntryProcessor implements EntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }
    }
}
