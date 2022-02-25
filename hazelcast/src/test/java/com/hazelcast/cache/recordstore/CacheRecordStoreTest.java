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

package com.hazelcast.cache.recordstore;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheRecordStoreTest
        extends CacheRecordStoreTestSupport {

    @Test
    public void putObjectAndGetDataFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.BINARY);
        putAndGetFromCacheRecordStore(cacheRecordStore, InMemoryFormat.BINARY);
    }

    @Test
    public void putObjectAndGetObjectFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.OBJECT);
        putAndGetFromCacheRecordStore(cacheRecordStore, InMemoryFormat.OBJECT);
    }

    @Test
    public void putObjectAndGetObjectExpiryPolicyFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.OBJECT);
        putAndSetExpiryPolicyFromRecordStore(cacheRecordStore, InMemoryFormat.OBJECT);
    }

    @Test
    public void putObjectAndGetDataExpiryPolicyFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.BINARY);
        putAndSetExpiryPolicyFromRecordStore(cacheRecordStore, InMemoryFormat.BINARY);
    }

}
