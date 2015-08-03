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

package com.hazelcast.hibernate;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Environment;
import org.hibernate.stat.SecondLevelCacheStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Read and write access (non-strict) cache concurrency strategy of Hibernate.
 * Data may be added, removed and mutated.
 * Nonstrict means that data integrity is not preserved as strictly as in READ_WRITE access
 * Cache invalidations are asychrnonous
 * Read through cache
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheHitMissNonStrictTest
        extends HibernateStatisticsTestSupport {

    protected String getCacheStrategy() {
        return AccessType.NONSTRICT_READ_WRITE.getExternalName();
    }

    @Override
    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        return props;
    }

    @Test
    public void testGetUpdateRemoveGet()
            throws Exception {
        insertDummyEntities(10, 4);
        //all 10 entities and 40 properties are cached
        SecondLevelCacheStatistics dummyEntityCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_ENTITY);
        SecondLevelCacheStatistics dummyPropertyCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_PROPERTY);

        sf.getCache().evictEntityRegions();
        sf.getCache().evictCollectionRegions();

        //miss 10 entities
        getDummyEntities(sf, 10);

        //hit 1 entity and 4 properties
        updateDummyEntityName(sf, 2, "updated");
        //invalidation is not synchronized, so we have to wait
        sleep(1);
        //entity 2 and its properties are invalidated

        //miss updated entity, hit 4 properties(they are still the same)
        getPropertiesOfEntity(sf, 2);

        //hit 1 entity and 4 properties
        deleteDummyEntity(sf, 1);

        assertEquals(12, dummyPropertyCacheStats.getHitCount());
        assertEquals(0, dummyPropertyCacheStats.getMissCount());
        assertEquals(2, dummyEntityCacheStats.getHitCount());
        assertEquals(11, dummyEntityCacheStats.getMissCount());
    }

    @Test
    public void testUpdateEventuallyInvalidatesObject() {
        insertDummyEntities(10, 4);
        //all 10 entities and 40 properties are cached
        final SecondLevelCacheStatistics dummyEntityCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_ENTITY);
        SecondLevelCacheStatistics dummyPropertyCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_PROPERTY);

        sf.getCache().evictEntityRegions();
        sf.getCache().evictCollectionRegions();

        //miss 10 entities
        getDummyEntities(sf, 10);

        //hit 1 entity and 4 properties
        updateDummyEntityName(sf, 2, "updated");
        assertSizeEventually(9, dummyEntityCacheStats.getEntries());
    }
}
