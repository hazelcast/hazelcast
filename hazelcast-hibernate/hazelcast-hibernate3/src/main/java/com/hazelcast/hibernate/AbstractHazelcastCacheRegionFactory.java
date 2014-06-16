/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.instance.HazelcastInstanceFactory;
import com.hazelcast.hibernate.instance.IHazelcastInstanceLoader;
import com.hazelcast.hibernate.region.HazelcastQueryResultsRegion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.QueryResultsRegion;
import org.hibernate.cache.RegionFactory;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cfg.Settings;

import java.util.Properties;

public abstract class AbstractHazelcastCacheRegionFactory implements RegionFactory {

    protected HazelcastInstance instance;
    private final ILogger logger = Logger.getLogger(getClass());

    private IHazelcastInstanceLoader instanceLoader;

    public AbstractHazelcastCacheRegionFactory() {
    }

    public AbstractHazelcastCacheRegionFactory(final Properties properties) {
        this();
    }

    public AbstractHazelcastCacheRegionFactory(final HazelcastInstance instance) {
        this.instance = instance;
    }

    public final QueryResultsRegion buildQueryResultsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastQueryResultsRegion(instance, regionName, properties);
    }

    /**
     * @return true - for a large cluster, unnecessary puts will most likely slow things down.
     */
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    public long nextTimestamp() {
        return HazelcastTimestamper.nextTimestamp(instance);
    }

    public void start(final Settings settings, final Properties properties) throws CacheException {
        logger.info("Starting up " + getClass().getSimpleName());
        if (instance == null || !instance.getLifecycleService().isRunning()) {
            instanceLoader = HazelcastInstanceFactory.createInstanceLoader(properties);
            instance = instanceLoader.loadInstance();
        }
    }

    public void stop() {
        if (instanceLoader != null) {
            logger.info("Shutting down " + getClass().getSimpleName());
            instanceLoader.unloadInstance();
            instance = null;
            instanceLoader = null;
        }
    }

    public HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    public AccessType getDefaultAccessType() {
        return AccessType.READ_WRITE;
    }
}
