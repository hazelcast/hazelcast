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

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.client.quorum.QuorumTestUtil.getClientConfig;
import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheReadQuorumTest extends HazelcastTestSupport {

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
        quorumConfig.setType(QuorumType.READ);
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
        c1 = factory.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.h5));
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
    public void testIteratorOperationSuccessfulWhenQuorumSizeMet() {
        cache1.iterator();
    }

    @Test(expected = QuorumException.class)
    public void testIteratorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.iterator();
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
}
