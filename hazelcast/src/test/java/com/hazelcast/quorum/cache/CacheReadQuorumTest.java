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
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheReadQuorumTest {

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
        quorumConfig.setType(QuorumType.READ);
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
        Future<String> foo = cache1.getAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAsync(1);
        foo.get();
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
}
