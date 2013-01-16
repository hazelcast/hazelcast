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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Settings;
import org.hibernate.engine.SessionFactoryImplementor;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Access underlying HazelcastInstance using Hibernate SessionFactory
 */
public abstract class HazelcastAccessor {

    static final ILogger logger = Logger.getLogger(HazelcastAccessor.class.getName());
    static final String METHOD_GET_REGION_FACTORY = "getRegionFactory";
    static final String METHOD_GET_CACHE_PROVIDER = "getCacheProvider";
    private static final String CLASS_CACHE_PROVIDER_ACCESSOR = "com.hazelcast.hibernate.instance.CacheProviderHazelcastAccessor";
    private static final String CLASS_REGION_FACTORY_ACCESSOR = "com.hazelcast.hibernate.instance.RegionFactoryHazelcastAccessor";

    private static final AtomicReference<HazelcastAccessor> OLD_HIBERNATE_ACCESSOR = new AtomicReference<HazelcastAccessor>();
    private static final AtomicReference<HazelcastAccessor> HIBERNATE_ACCESSOR = new AtomicReference<HazelcastAccessor>();

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
            logger.log(Level.WARNING, "SessionFactory is expected to be instance of SessionFactoryImplementor.");
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
        boolean oldHibernateVersion = false;
        try {
            // check existence of Settings.getRegionFactory() method
            sessionFactory.getSettings().getClass().getMethod(METHOD_GET_REGION_FACTORY);
        } catch (NoSuchMethodException ignore) {
            oldHibernateVersion = true;
        }
        final HazelcastAccessor accessor;
        try {
            if (oldHibernateVersion) {
                accessor = getAccessor(OLD_HIBERNATE_ACCESSOR, CLASS_CACHE_PROVIDER_ACCESSOR);
            } else {
                accessor = getAccessor(HIBERNATE_ACCESSOR, CLASS_REGION_FACTORY_ACCESSOR);
            }
            return accessor.getHazelcastInstance(sessionFactory.getSettings());
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    private static HazelcastAccessor getAccessor(AtomicReference<HazelcastAccessor> ref, final String className)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        HazelcastAccessor accessor = ref.get();
        // No need to be pessimistic about concurrent access
        if (accessor == null) {
            final ClassLoader cl = HazelcastAccessor.class.getClassLoader();
            accessor = (HazelcastAccessor) cl.loadClass(className).newInstance();
            ref.set(accessor);
        }
        return accessor;
    }

    HazelcastAccessor() {
    }

    abstract HazelcastInstance getHazelcastInstance(final Settings settings);
}
