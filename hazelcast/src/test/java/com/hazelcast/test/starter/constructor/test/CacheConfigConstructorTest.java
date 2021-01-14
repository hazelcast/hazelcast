/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.CacheConfigConstructor;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheConfigConstructorTest {

    @Test
    @Ignore("broken due to compatibility code expecting 3.12 target class/classloader")
    public void testConstructor() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName("myCache");
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setBackupCount(1);
        cacheConfig.setAsyncBackupCount(2);

        CacheConfigConstructor constructor = new CacheConfigConstructor(CacheConfig.class);
        CacheConfig clonedCacheConfig = (CacheConfig) constructor.createNew(cacheConfig);

        assertEquals(cacheConfig.getName(), clonedCacheConfig.getName());
        assertEquals(cacheConfig.getNameWithPrefix(), clonedCacheConfig.getNameWithPrefix());
        assertEquals(cacheConfig.getInMemoryFormat(), clonedCacheConfig.getInMemoryFormat());
        assertEquals(cacheConfig.getBackupCount(), clonedCacheConfig.getBackupCount());
        assertEquals(cacheConfig.getAsyncBackupCount(), clonedCacheConfig.getAsyncBackupCount());
    }
}
