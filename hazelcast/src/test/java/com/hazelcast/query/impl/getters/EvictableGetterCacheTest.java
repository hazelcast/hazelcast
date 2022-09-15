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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictableGetterCacheTest {

    @Test
    public void when_getAndPut_then_correctResult() {
        // GIVEN
        EvictableGetterCache cache = new EvictableGetterCache(10, 10, 0.5f, true);
        Getter x = mock(Getter.class);
        Getter y = mock(Getter.class);

        // WHEN
        cache.putGetter(String.class, "x", x);
        cache.putGetter(String.class, "y", y);

        // THEN
        assertThat(cache.getGetter(String.class, "x"), equalTo(x));
        assertThat(cache.getGetter(String.class, "y"), equalTo(y));
    }

    @Test
    public void when_getAndPut_then_correctSize() {
        // GIVEN
        EvictableGetterCache cache = new EvictableGetterCache(10, 10, 0.5f, true);
        Getter x = mock(Getter.class);
        Getter y = mock(Getter.class);

        // WHEN
        cache.putGetter(String.class, "x", x);
        cache.putGetter(Double.class, "y", y);

        // THEN
        assertThat(cache.getClassCacheSize(), equalTo(2));
        assertThat(cache.getGetterPerClassCacheSize(String.class), equalTo(1));
        assertThat(cache.getGetterPerClassCacheSize(Double.class), equalTo(1));
    }

    @Test
    public void when_getterCacheEvictLimitNotReached_then_noEviction() {
        // GIVEN
        int getterCacheSize = 10;
        float evictPercentage = 0.3f;
        EvictableGetterCache cache = new EvictableGetterCache(10, getterCacheSize, evictPercentage, true);

        // WHEN
        for (int i = 0; i < getterCacheSize - 1; i++) {
            cache.putGetter(String.class, "x" + i, mock(Getter.class));
        }

        // THEN
        assertThat(cache.getClassCacheSize(), equalTo(1));
        assertThat(cache.getGetterPerClassCacheSize(String.class), equalTo(getterCacheSize - 1));
    }

    @Test
    public void when_getterCacheEvictLimitReached_then_evictionTriggered() {
        // GIVEN
        int getterCacheSize = 10;
        float evictPercentage = 0.3f;
        EvictableGetterCache cache = new EvictableGetterCache(10, getterCacheSize, evictPercentage, true);

        // WHEN
        for (int i = 0; i < getterCacheSize; i++) {
            cache.putGetter(String.class, "x" + i, mock(Getter.class));
        }

        // THEN
        int expectedSizeAfterEviction = (int) (getterCacheSize * (1 - evictPercentage));
        assertThat(cache.getClassCacheSize(), equalTo(1));
        assertThat(cache.getGetterPerClassCacheSize(String.class), equalTo(expectedSizeAfterEviction));
    }

    @Test
    public void when_classCacheEvictLimitNotReached_then_noEviction() {
        // GIVEN
        int classCacheSize = 10;
        float evictPercentage = 0.3f;
        EvictableGetterCache cache = new EvictableGetterCache(classCacheSize, 10, evictPercentage, true);
        Class<?>[] classes = {
                String.class, Character.class, Integer.class, Double.class, Byte.class, Long.class,
                Number.class, Float.class, BigDecimal.class, BigInteger.class,
        };

        // WHEN
        for (int i = 0; i < classCacheSize - 1; i++) {
            cache.putGetter(classes[i], "x", mock(Getter.class));
        }

        // THEN
        assertThat(cache.getClassCacheSize(), equalTo(classCacheSize - 1));
    }

    @Test
    public void when_classCacheEvictLimitReached_then_evictionTriggered() {
        // GIVEN
        int classCacheSize = 10;
        float evictPercentage = 0.3f;
        EvictableGetterCache cache = new EvictableGetterCache(classCacheSize, 10, evictPercentage, true);
        Class<?>[] classes = {
                String.class, Character.class, Integer.class, Double.class, Byte.class, Long.class,
                Number.class, Float.class, BigDecimal.class, BigInteger.class,
        };

        // WHEN
        for (int i = 0; i < classCacheSize; i++) {
            cache.putGetter(classes[i], "x", mock(Getter.class));
        }

        // THEN
        int expectedSizeAfterEviction = (int) (classCacheSize * (1 - evictPercentage));
        assertThat(cache.getClassCacheSize(), equalTo(expectedSizeAfterEviction));
    }
}
