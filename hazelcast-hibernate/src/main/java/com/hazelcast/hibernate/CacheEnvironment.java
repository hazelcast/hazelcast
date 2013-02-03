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

import org.hibernate.cfg.Environment;
import org.hibernate.util.PropertiesHelper;
import org.hibernate.util.StringHelper;

import java.util.Properties;

public final class CacheEnvironment {

    public static final String CONFIG_FILE_PATH_LEGACY = Environment.CACHE_PROVIDER_CONFIG;

    public static final String CONFIG_FILE_PATH = "hibernate.cache.hazelcast.configuration_file_path";

    @Deprecated
    public static final String USE_SUPER_CLIENT = "hibernate.cache.hazelcast.use_super_client";

    public static final String USE_LITE_MEMBER = "hibernate.cache.hazelcast.use_lite_member";

    public static final String USE_NATIVE_CLIENT = "hibernate.cache.hazelcast.use_native_client";

    @Deprecated
    public static final String NATIVE_CLIENT_HOSTS = "hibernate.cache.hazelcast.native_client_hosts";

    public static final String NATIVE_CLIENT_ADDRESS = "hibernate.cache.hazelcast.native_client_address";

    public static final String NATIVE_CLIENT_GROUP = "hibernate.cache.hazelcast.native_client_group";

    public static final String NATIVE_CLIENT_PASSWORD = "hibernate.cache.hazelcast.native_client_password";

    public static final String SHUTDOWN_ON_STOP = "hibernate.cache.hazelcast.shutdown_on_session_factory_close";

    @Deprecated
    public static final String LOCK_TIMEOUT_SECONDS = "hibernate.cache.hazelcast.lock_timeout_in_seconds";

    public static final String LOCK_TIMEOUT = "hibernate.cache.hazelcast.lock_timeout";

    public static final String HAZELCAST_INSTANCE_NAME = "hibernate.cache.hazelcast.instance_name";

    private static final int MAXIMUM_LOCK_TIMEOUT = 10000; // milliseconds

    private final static int DEFAULT_CACHE_TIMEOUT = (3600 * 1000); // one hour in milliseconds

    public static final String EXPLICIT_VERSION_CHECK = "hibernate.cache.hazelcast.explicit_version_check";

    public static String getConfigFilePath(Properties props) {
        String configResourcePath = PropertiesHelper.getString(CacheEnvironment.CONFIG_FILE_PATH_LEGACY, props, null);
        if (StringHelper.isEmpty(configResourcePath)) {
            configResourcePath = PropertiesHelper.getString(CacheEnvironment.CONFIG_FILE_PATH, props, null);
        }
        return configResourcePath;
    }

    public static String getInstanceName(Properties props) {
        return PropertiesHelper.getString(HAZELCAST_INSTANCE_NAME, props, null);
    }

    public static boolean isNativeClient(Properties props) {
        return PropertiesHelper.getBoolean(CacheEnvironment.USE_NATIVE_CLIENT, props, false);
    }

    public static boolean isLiteMember(Properties props) {
        return PropertiesHelper.getBoolean(CacheEnvironment.USE_LITE_MEMBER, props, false);
    }

    @Deprecated
    public static boolean isSuperClient(Properties props) {
        return PropertiesHelper.getBoolean(CacheEnvironment.USE_SUPER_CLIENT, props, false);
    }

    public static int getDefaultCacheTimeoutInMillis() {
        return DEFAULT_CACHE_TIMEOUT;
    }

    public static int getLockTimeoutInMillis(Properties props) {
        int timeout = -1;
        try {
            timeout = PropertiesHelper.getInt(LOCK_TIMEOUT, props, -1);
        } catch (Exception ignored) {
        }
        if (timeout < 0) {
            try {
                timeout = PropertiesHelper.getInt(LOCK_TIMEOUT_SECONDS, props, -1) * 1000;
            } catch (Exception ignored) {
            }
        }
        if (timeout < 0) {
            timeout = MAXIMUM_LOCK_TIMEOUT;
        }
        return timeout;
    }

    public static boolean shutdownOnStop(Properties props, boolean defaultValue) {
        return PropertiesHelper.getBoolean(CacheEnvironment.SHUTDOWN_ON_STOP, props, defaultValue);
    }

    public static boolean isExplicitVersionCheckEnabled(Properties props) {
        return PropertiesHelper.getBoolean(CacheEnvironment.EXPLICIT_VERSION_CHECK, props, false);
    }
}
