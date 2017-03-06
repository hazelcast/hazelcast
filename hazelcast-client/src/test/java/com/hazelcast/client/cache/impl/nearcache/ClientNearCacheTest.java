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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientNearCacheTest extends ClientNearCacheTestSupport {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientNearCache() {
        putAndGetFromCacheAndThenGetFromClientNearCache(inMemoryFormat);
    }

    @Test
    public void putToCacheAndThenGetFromClientNearCache() {
        putToCacheAndThenGetFromClientNearCache(inMemoryFormat);
    }

    @Test
    public void putIfAbsentToCacheAndThenGetFromClientNearCache() {
        putIfAbsentToCacheAndThenGetFromClientNearCache(inMemoryFormat);
    }

    @Test
    @Ignore(value = "https://github.com/hazelcast/hazelcast/issues/10031")
    public void putToCacheAndGetInvalidationEventWhenNodeShutdown() {
        putToCacheAndGetInvalidationEventWhenNodeShutdown(inMemoryFormat);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(inMemoryFormat);
    }

    @Test
    public void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache() {
        putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(inMemoryFormat);
    }

    @Test
    public void testLoadAllNearCacheInvalidationBinary() throws Exception {
        testLoadAllNearCacheInvalidation(inMemoryFormat);
    }

    @Test
    public void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache() {
        putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(inMemoryFormat);
    }

    @Test
    public void testGetAllReturnsFromNearCache() {
        doTestGetAllReturnsFromNearCache(inMemoryFormat);
    }

    @Test
    public void putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled() {
        putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled(inMemoryFormat);
    }

    @Test
    public void putAsyncToCacheAndThenGetFromClientNearCacheImmediately() throws Exception {
        putAsyncToCacheAndThenGetFromClientNearCacheImmediately(inMemoryFormat);
    }

    @Test
    public void testNearCacheTTLRecordsExpired_() {
        testNearCacheExpiration_withTTL(inMemoryFormat);
    }

    @Test
    public void testNearCacheIdleRecordsExpired_() {
        testNearCacheExpiration_withMaxIdle(inMemoryFormat);
    }
}
