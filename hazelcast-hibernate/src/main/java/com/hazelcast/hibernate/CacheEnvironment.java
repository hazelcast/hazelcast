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

import org.hibernate.cfg.Environment;

public final class CacheEnvironment {
	
	public static final String CONFIG_FILE_PATH_LEGACY = Environment.CACHE_PROVIDER_CONFIG;
	
	public static final String CONFIG_FILE_PATH = "hibernate.cache.hazelcast.configuration_file_path";
	
	public static final String USE_SUPER_CLIENT = "hibernate.cache.hazelcast.use_super_client";
	
	public static final String USE_NATIVE_CLIENT = "hibernate.cache.hazelcast.use_native_client";
	
	public static final String NATIVE_CLIENT_HOSTS = "hibernate.cache.hazelcast.native_client_hosts";
	
	public static final String NATIVE_CLIENT_GROUP = "hibernate.cache.hazelcast.native_client_group";
	
	public static final String NATIVE_CLIENT_PASSWORD = "hibernate.cache.hazelcast.native_client_password";
	
	public static final String LOCK_TIMEOUT = "hibernate.cache.hazelcast.lock_timeout_in_seconds";
	
	public static final String EXPLICIT_VERSION_CHECK = "hibernate.cache.hazelcast.explicit_version_check";
}
