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

package com.hazelcast.hibernate;

import java.util.Properties;

import org.hibernate.cfg.Environment;
import org.hibernate.util.PropertiesHelper;
import org.hibernate.util.StringHelper;

public final class CacheEnvironment {
	
	public static final String CONFIG_FILE_PATH_LEGACY = Environment.CACHE_PROVIDER_CONFIG;
	
	public static final String CONFIG_FILE_PATH = "hibernate.cache.hazelcast.configuration_file_path";
	
	public static final String USE_SUPER_CLIENT = "hibernate.cache.hazelcast.use_super_client";
	
	public static final String USE_NATIVE_CLIENT = "hibernate.cache.hazelcast.use_native_client";
	
	public static final String NATIVE_CLIENT_HOSTS = "hibernate.cache.hazelcast.native_client_hosts";
	
	public static final String NATIVE_CLIENT_GROUP = "hibernate.cache.hazelcast.native_client_group";
	
	public static final String NATIVE_CLIENT_PASSWORD = "hibernate.cache.hazelcast.native_client_password";
	
	public static final String SHUTDOWN_ON_STOP = "hibernate.cache.hazelcast.shutdown_on_session_factory_close";
	
	public static final String LOCK_TIMEOUT = "hibernate.cache.hazelcast.lock_timeout_in_seconds";
	
	public static final String HAZELCAST_INSTANCE_NAME = "hibernate.cache.hazelcast.instance_name";
	
	private static final int MAXIMUM_LOCK_TIMEOUT = 600; // seconds
	
	private final static int DEFAULT_CACHE_TIMEOUT = (3600 * 1000); // one hour in milliseconds
	
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
	
	public static boolean isSuperClient(Properties props) {
		return PropertiesHelper.getBoolean(CacheEnvironment.USE_SUPER_CLIENT, props, false);
	}
	
	public static int getDefaultCacheTimeoutInMillis() {
		return DEFAULT_CACHE_TIMEOUT;
	}
	
	public static int getLockTimeoutInSeconds(Properties props) {
		try {
			return PropertiesHelper.getInt(LOCK_TIMEOUT, props, MAXIMUM_LOCK_TIMEOUT);
		} catch (Exception ignored) {
		}
		return MAXIMUM_LOCK_TIMEOUT;
	}
	
	public static boolean shutdownOnStop(Properties props, boolean defaultValue) {
		return PropertiesHelper.getBoolean(CacheEnvironment.SHUTDOWN_ON_STOP, props, defaultValue);
	}
}
