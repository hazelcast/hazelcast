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

package com.hazelcast.hibernate.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.AbstractHazelcastCacheRegionFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cfg.Settings;
import org.hibernate.engine.spi.SessionFactoryImplementor;

/**
 * Access underlying HazelcastInstance using Hibernate SessionFactory
 */
public final class HazelcastAccessor {

    static final ILogger LOGGER = Logger.getLogger(HazelcastAccessor.class);

    private HazelcastAccessor() {
    }

    /**
     * Tries to extract <code>HazelcastInstance</code> from <code>Session</code>.
     *
     * @param session
     * @return Currently used <code>HazelcastInstance</code> or null if an error occurs.
     */
    public static HazelcastInstance getHazelcastInstance(final Session session) {
        return getHazelcastInstance(session.getSessionFactory());
    }

    /**
     * Tries to extract <code>HazelcastInstance</code> from <code>SessionFactory</code>.
     *
     * @param sessionFactory
     * @return Currently used <code>HazelcastInstance</code> or null if an error occurs.
     */
    public static HazelcastInstance getHazelcastInstance(final SessionFactory sessionFactory) {
        if (!(sessionFactory instanceof SessionFactoryImplementor)) {
            LOGGER.warning("SessionFactory is expected to be instance of SessionFactoryImplementor.");
            return null;
        }
        return getHazelcastInstance((SessionFactoryImplementor) sessionFactory);
    }

    /**
     * Tries to extract <code>HazelcastInstance</code> from <code>SessionFactoryImplementor</code>.
     *
     * @param sessionFactory
     * @return currently used <code>HazelcastInstance</code> or null if an error occurs.
     */
    public static HazelcastInstance getHazelcastInstance(final SessionFactoryImplementor sessionFactory) {
        final Settings settings = sessionFactory.getSettings();
        final RegionFactory rf = settings.getRegionFactory();
        if (rf == null) {
            LOGGER.severe("Hibernate 2nd level cache has not been enabled!");
            return null;
        }
        if (rf instanceof AbstractHazelcastCacheRegionFactory) {
            return ((AbstractHazelcastCacheRegionFactory) rf).getHazelcastInstance();
        } else {
            LOGGER.warning("Current 2nd level cache implementation is not HazelcastCacheRegionFactory!");
        }
        return null;
    }
}
