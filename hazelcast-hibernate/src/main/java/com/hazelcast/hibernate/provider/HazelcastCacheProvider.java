/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.hibernate.provider;

import java.util.Properties;
import java.util.logging.Level;

import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CacheProvider;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Implementation of (deprecated) Hibernate <code>CacheProvider</code> interface for compatibility with pre-Hibernate
 * 3.3.x code.
 * <p/>
 * To enable, <code>hibernate.cache.provider_class=com.hazelcast.hibernate.provider.HazelcastCacheProvider</code>. This
 * cache provider relies on <code>hazelcast.xml</code> for cache configuration.
 *
 * @author Leo Kim (lkim@limewire.com)
 * @see HazelcastCache
 * @see HazelcastCacheRegionFactory
 */
public final class HazelcastCacheProvider implements CacheProvider {

    private static final ILogger LOG = Logger.getLogger(HazelcastCacheProvider.class.getName());
    private final IdGenerator idGenerator;

    public HazelcastCacheProvider() {
        idGenerator = Hazelcast.getIdGenerator("HazelcastCacheProviderTimestampIdGenerator");
    }

    /**
     * We ignore the <code>Properties</code> passed in here in favor of the <code>hazelcast.xml</code> file.
     */
    public Cache buildCache(final String name, final Properties properties) throws CacheException {
        return new HazelcastCache(name);
    }

    /**
     * From what I can tell from the <code>{@link org.hibernate.cache.CacheCurrencyStrategy}</code>s implemented in
     * Hibernate, the return value "false" will mean an object will be replaced in a cache if it already exists there,
     * and "true" will not replace it.
     *
     * @return true - for a large cluster, unnecessary puts will most likely slow things down.
     */
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    /**
     * @return Output of <code>{@link Hazelcast#getIdGenerator}</code> and <code>{@link IdGenerator#newId()}</code>
     */
    public long nextTimestamp() {
        final long id = idGenerator.newId();
        return id;
    }

    public void start(final Properties arg0) throws CacheException {
        LOG.log(Level.INFO, "Starting up HazelcastCacheProvider...");
    }

    /**
     * Calls <code>{@link Hazelcast#shutdown()}</code>.
     */
    public void stop() {
        LOG.log(Level.INFO, "Shutting down HazelcastCacheProvider...");
        Hazelcast.shutdown();
    }
}
