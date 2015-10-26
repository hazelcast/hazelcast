/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheEntryViews;
import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheEntryViewsTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testPrivateConstructor() {
        HazelcastTestSupport.assertUtilityConstructor(CacheEntryViews.class);
    }

    private void doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType cacheEntryViewType) {
        String key = "testKey";
        String value = "testValue";
        CacheObjectRecord record = new CacheObjectRecord(value, System.currentTimeMillis(), 1234L);
        CacheEntryView cacheEntryView =
                CacheEntryViews.createEntryView(serializationService.toData(key),
                        serializationService.toData(value),
                        record,
                        cacheEntryViewType);

        assertEquals(key, serializationService.toObject(cacheEntryView.getKey()));
        assertEquals(value, serializationService.toObject(cacheEntryView.getValue()));
        assertEquals(record.getAccessHit(), cacheEntryView.getAccessHit());
        assertEquals(record.getExpirationTime(), cacheEntryView.getExpirationTime());
        assertEquals(record.getAccessTime(), cacheEntryView.getLastAccessTime());
    }

    @Test
    public void testDefaultCacheEntryView() {
        doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType.DEFAULT);
    }

    @Test
    public void testLazyCacheEntryView() {
        doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType.LAZY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCacheEntryView() {
        doCacheEntryViewTest(null);
    }

}
