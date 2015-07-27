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

import com.hazelcast.hibernate.access.ReadOnlyAccessDelegate;
import com.hazelcast.hibernate.region.HazelcastRegion;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cfg.Environment;
import org.hibernate.stat.CollectionStatistics;
import org.hibernate.stat.SecondLevelCacheStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheHitMissReadOnlyTest extends HibernateStatisticsTestSupport {

    protected String getCacheStrategy() {
        return AccessType.READ_ONLY.getName();
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
        CollectionStatistics collectionCacheStats = sf.getStatistics().getCollectionStatistics(CACHE_COLLECTION_ENTITY);
        SecondLevelCacheStatistics dummyPropertyCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_PROPERTY);

        sf.getCache().evictEntityRegions();
        sf.getCache().evictCollectionRegions();
        //miss 10 entities
        getDummyEntities(sf, 10);
        //hit 1 entity and 4 properties
        deleteDummyEntity(sf, 1);
        sleep(1);

        assertEquals(4, dummyPropertyCacheStats.getHitCount());
        assertEquals(0, dummyPropertyCacheStats.getMissCount());
        assertEquals(1, dummyEntityCacheStats.getHitCount());
        assertEquals(10, dummyEntityCacheStats.getMissCount());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateQueryCausesInvalidationOfEntireRegion() {
        insertDummyEntities(10);
        executeUpdateQuery(sf, "UPDATE DummyEntity set name = 'manually-updated' where id=2");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadOnlyUpdate() throws Exception{
        insertDummyEntities(1, 0);
        updateDummyEntityName(sf, 0, "updated");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAfterUpdateShouldThrowOnReadOnly() {
        HazelcastRegion hzRegion = mock(HazelcastRegion.class);
        when(hzRegion.getCache()).thenReturn(null);
        when(hzRegion.getLogger()).thenReturn(null);
        ReadOnlyAccessDelegate readOnlyAccessDelegate = new ReadOnlyAccessDelegate(hzRegion, null);
        readOnlyAccessDelegate.afterUpdate(null, null, null, null, null);
    }
}
