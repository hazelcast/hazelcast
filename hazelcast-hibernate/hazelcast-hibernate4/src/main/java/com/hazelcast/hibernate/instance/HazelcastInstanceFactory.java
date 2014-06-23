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
import com.hazelcast.hibernate.CacheEnvironment;
import org.hibernate.cache.CacheException;

import java.util.Properties;

/**
 * A factory class to build up a {@link com.hazelcast.core.HazelcastInstance} depending
 * on configuration either based on Hazelcast client or node implementation.
 */
public final class HazelcastInstanceFactory {

    private static final String HZ_CLIENT_LOADER_CLASSNAME = "com.hazelcast.hibernate.instance.HazelcastClientLoader";
    private static final String HZ_INSTANCE_LOADER_CLASSNAME = "com.hazelcast.hibernate.instance.HazelcastInstanceLoader";

    private HazelcastInstanceFactory() {
    }

    public static HazelcastInstance createInstance(Properties props) throws CacheException {
        return createInstanceLoader(props).loadInstance();
    }

    public static IHazelcastInstanceLoader createInstanceLoader(Properties props) throws CacheException {
        boolean useNativeClient = false;
        if (props != null) {
            useNativeClient = CacheEnvironment.isNativeClient(props);
        }
        IHazelcastInstanceLoader loader = null;
        Class loaderClass = null;
        ClassLoader cl = HazelcastInstanceFactory.class.getClassLoader();
        try {
            if (useNativeClient) {
                loaderClass = cl.loadClass(HZ_CLIENT_LOADER_CLASSNAME);
            } else {
                loaderClass = cl.loadClass(HZ_INSTANCE_LOADER_CLASSNAME);
            }
            loader = (IHazelcastInstanceLoader) loaderClass.newInstance();
        } catch (Exception e) {
            throw new CacheException(e);
        }
        loader.configure(props);
        return loader;
    }
}
