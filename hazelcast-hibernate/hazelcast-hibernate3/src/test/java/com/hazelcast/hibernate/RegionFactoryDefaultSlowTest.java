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

import com.hazelcast.config.MapConfig;
import com.hazelcast.hibernate.region.HazelcastQueryResultsRegion;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.hibernate.cfg.Environment;
import org.hibernate.impl.SessionFactoryImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RegionFactoryDefaultSlowTest extends HibernateSlowTestSupport {

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        return props;
    }

    @Test
    public void testQueryCacheCleanup() {
        MapConfig mapConfig = getHazelcastInstance(sf).getConfig().getMapConfig("org.hibernate.cache.*");
        final float baseEvictionRate = 0.2f;
        final int numberOfEntities = 100;
        final int defaultCleanupPeriod = 60;
        final int maxSize = mapConfig.getMaxSizeConfig().getSize();
        final int evictedItemCount = numberOfEntities - maxSize + (int) (maxSize * baseEvictionRate);
        insertDummyEntities(numberOfEntities);
        sleep(1);
        for (int i = 0; i < numberOfEntities; i++) {
            executeQuery(sf, i);
        }

        HazelcastQueryResultsRegion queryRegion = ((HazelcastQueryResultsRegion) (((SessionFactoryImpl) sf).getQueryCache()).getRegion());
        assertEquals(numberOfEntities, queryRegion.getCache().size());
        sleep(defaultCleanupPeriod);

        assertEquals(numberOfEntities - evictedItemCount, queryRegion.getCache().size());
    }

}
