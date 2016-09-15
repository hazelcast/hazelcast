/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheTest extends ClientNearCacheTestSupport {

    @Test
    public void putAndGetFromCacheAndThenGetFromClientNearCacheWithBinaryInMemoryFormat() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientNearCacheWithObjectInMemoryFormat() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void putToCacheAndThenGetFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndThenGetFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void putIfAbsentToCacheAndThenGetFromClientNearCacheWithBinaryInMemoryFormat() {
        putIfAbsentToCacheAndThenGetFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putIfAbsentToCacheAndThenGetFromClientNearCacheWithObjectInMemoryFormat() {
        putIfAbsentToCacheAndThenGetFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void putToCacheAndGetInvalidationEventWhenNodeShutdownWithBinaryInMemoryFormat() {
        putToCacheAndGetInvalidationEventWhenNodeShutdown(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndGetInvalidationEventWhenNodeShutdownWithObjectInMemoryFormat() {
        putToCacheAndGetInvalidationEventWhenNodeShutdown(InMemoryFormat.OBJECT);
    }

    @Test
    public void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void testLoadAllNearCacheInvalidationBinary() throws Exception {
        testLoadAllNearCacheInvalidation(InMemoryFormat.BINARY);
    }

    @Test
    public void testLoadAllNearCacheInvalidationObject() throws Exception {
        testLoadAllNearCacheInvalidation(InMemoryFormat.OBJECT);
    }

    @Test
    public void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat.OBJECT);
    }

    @Test
    public void testGetAllReturnsFromNearCache() {
        doTestGetAllReturnsFromNearCache();
    }

    @Test
    public void putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabledWithBinaryInMemoryFormat() {
        putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabledWithObjectInMemoryFormat() {
        putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAsyncToCacheAndThenGetFromClientNearCacheImmediatelyWithBinaryInMemoryFormat() throws Exception {
        putAsyncToCacheAndThenGetFromClientNearCacheImmediately(InMemoryFormat.BINARY);
    }

    @Test
    public void putAsyncToCacheAndThenGetFromClientNearCacheImmediatelyWithObjectInMemoryFormat() throws Exception {
        putAsyncToCacheAndThenGetFromClientNearCacheImmediately(InMemoryFormat.OBJECT);
    }

    @Test
    public void testNearCacheEviction_withObjectInMemoryFormat() {
        testNearCacheEviction(InMemoryFormat.OBJECT);
    }

    @Test
    public void testNearCacheEviction_withBinaryInMemoryFormat() {
        testNearCacheEviction(InMemoryFormat.BINARY);
    }

    @Test
    public void testNearCacheTTLRecordsExpired_withObjectInMemoryFormat() {
        testNearCacheExpiration_withTTL(InMemoryFormat.OBJECT);
    }

    @Test
    public void testNearCacheTTLRecordsExpired_withBinaryInMemoryFormat() {
        testNearCacheExpiration_withTTL(InMemoryFormat.BINARY);
    }

    @Test
    public void testNearCacheIdleRecordsExpired_withObjectInMemoryFormat() {
        testNearCacheExpiration_withMaxIdle(InMemoryFormat.OBJECT);
    }

    @Test
    public void testNearCacheIdleRecordsExpired_withBinaryInMemoryFormat() {
        testNearCacheExpiration_withMaxIdle(InMemoryFormat.BINARY);
    }

    @Test
    public void testNearCacheMemoryCostCalculation_withObjectInMemoryFormat() {
        testNearCacheMemoryCostCalculation(InMemoryFormat.OBJECT, 1);
    }

    @Test
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses_withObjectInMemoryFormat() {
        testNearCacheMemoryCostCalculation(InMemoryFormat.OBJECT, 10);
    }

    @Test
    public void testNearCacheMemoryCostCalculation_withBinaryInMemoryFormat() {
        testNearCacheMemoryCostCalculation(InMemoryFormat.BINARY, 1);
    }

    @Test
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses_withBinaryInMemoryFormat() {
        testNearCacheMemoryCostCalculation(InMemoryFormat.BINARY, 10);
    }
}
