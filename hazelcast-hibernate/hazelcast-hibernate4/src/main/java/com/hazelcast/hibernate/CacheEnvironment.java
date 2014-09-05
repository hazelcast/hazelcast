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

import com.hazelcast.logging.Logger;
import org.hibernate.cfg.Environment;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;

import java.util.Properties;

/**
 * This class is used to help in setup the internal caches. It searches for configuration files
 * and contains all property names for hibernate based configuration properties.
 */
public final class CacheEnvironment {

    /**
     * Legacy property to configure the path of the hazelcast.xml or hazelcast-client.xml configuration files
     */
    @Deprecated
    public static final String CONFIG_FILE_PATH_LEGACY = Environment.CACHE_PROVIDER_CONFIG;

    /**
     * Property to configure the path of the hazelcast.xml or hazelcast-client.xml configuration files
     */
    public static final String CONFIG_FILE_PATH = "hibernate.cache.hazelcast.configuration_file_path";

    /**
     * Property to configure weather a Hazelcast client or node will be used for connection to the cluster
     */
    public static final String USE_NATIVE_CLIENT = "hibernate.cache.hazelcast.use_native_client";

    /**
     * Property to configure the address for the Hazelcast client to connect to
     */
    public static final String NATIVE_CLIENT_ADDRESS = "hibernate.cache.hazelcast.native_client_address";

    /**
     * Property to configure Hazelcast client group name
     */
    public static final String NATIVE_CLIENT_GROUP = "hibernate.cache.hazelcast.native_client_group";

    /**
     * Property to configure Hazelcast client group password
     */
    public static final String NATIVE_CLIENT_PASSWORD = "hibernate.cache.hazelcast.native_client_password";

    /**
     * Property to configure if the HazelcastInstance should going to shutdown when the RegionFactory is being stopped
     */
    public static final String SHUTDOWN_ON_STOP = "hibernate.cache.hazelcast.shutdown_on_session_factory_close";

    /**
     * Property to configure the timeout delay before a lock eventually times out
     */
    public static final String LOCK_TIMEOUT = "hibernate.cache.hazelcast.lock_timeout";

    /**
     * Property to configure the Hazelcast instance internal name
     */
    public static final String HAZELCAST_INSTANCE_NAME = "hibernate.cache.hazelcast.instance_name";

    /**
     * Property to configure explicitly checks the CacheEntry's version while updating RegionCache.
     * If new entry's version is not higher then previous, update will be cancelled.
     */
    public static final String EXPLICIT_VERSION_CHECK = "hibernate.cache.hazelcast.explicit_version_check";

    // milliseconds
    private static final int MAXIMUM_LOCK_TIMEOUT = 10000;

    // one hour in milliseconds
    private static final int DEFAULT_CACHE_TIMEOUT = (3600 * 1000);

    private CacheEnvironment() {
    }

    public static String getConfigFilePath(Properties props) {
        String configResourcePath = ConfigurationHelper.getString(CacheEnvironment.CONFIG_FILE_PATH_LEGACY, props, null);
        if (StringHelper.isEmpty(configResourcePath)) {
            configResourcePath = ConfigurationHelper.getString(CacheEnvironment.CONFIG_FILE_PATH, props, null);
        }
        return configResourcePath;
    }

    public static String getInstanceName(Properties props) {
        return ConfigurationHelper.getString(HAZELCAST_INSTANCE_NAME, props, null);
    }

    public static boolean isNativeClient(Properties props) {
        return ConfigurationHelper.getBoolean(CacheEnvironment.USE_NATIVE_CLIENT, props, false);
    }

    public static int getDefaultCacheTimeoutInMillis() {
        return DEFAULT_CACHE_TIMEOUT;
    }

    public static int getLockTimeoutInMillis(Properties props) {
        int timeout = -1;
        try {
            timeout = ConfigurationHelper.getInt(LOCK_TIMEOUT, props, -1);
        } catch (Exception e) {
            Logger.getLogger(CacheEnvironment.class).finest(e);
        }
        if (timeout < 0) {
            timeout = MAXIMUM_LOCK_TIMEOUT;
        }
        return timeout;
    }

    public static boolean shutdownOnStop(Properties props, boolean defaultValue) {
        return ConfigurationHelper.getBoolean(CacheEnvironment.SHUTDOWN_ON_STOP, props, defaultValue);
    }

    public static boolean isExplicitVersionCheckEnabled(Properties props) {
        return ConfigurationHelper.getBoolean(CacheEnvironment.EXPLICIT_VERSION_CHECK, props, false);
    }
}
