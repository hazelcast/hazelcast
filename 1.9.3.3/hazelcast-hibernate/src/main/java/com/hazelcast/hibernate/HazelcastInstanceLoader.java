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

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;

import org.hibernate.cache.CacheException;
import org.hibernate.util.ConfigHelper;
import org.hibernate.util.PropertiesHelper;
import org.hibernate.util.StringHelper;

import com.hazelcast.config.Config;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

class HazelcastInstanceLoader implements IHazelcastInstanceLoader {

	private final static ILogger logger = Logger
			.getLogger(HazelcastInstanceFactory.class.getName());

	public HazelcastInstance loadInstance(Properties props)	throws CacheException {
		boolean useSuperClient = false;
		String configResourcePath = null;

		if (props != null) {
			useSuperClient = PropertiesHelper.getBoolean(CacheEnvironment.USE_SUPER_CLIENT, props, false);
			configResourcePath = PropertiesHelper.getString(CacheEnvironment.CONFIG_FILE_PATH_LEGACY, props, null);
			if (StringHelper.isEmpty(configResourcePath)) {
				configResourcePath = PropertiesHelper.getString(CacheEnvironment.CONFIG_FILE_PATH, props, null);
			}
		}

		if (useSuperClient) {
			logger.log(Level.WARNING,
					"Creating Hazelcast node as Super-Client. "
					+ "Be sure this node has access to an already running cluster...");
		}

		Config config = null;
		if (StringHelper.isEmpty(configResourcePath)) {
			// If HazelcastInstance will not be super-client
			// then just return default instance. 
			// We do not need to edit configuration. 
			if(!useSuperClient) {
				return Hazelcast.getDefaultInstance();
			}
			
			// Load config from hazelcast-default.xml
			// And set super client property.
			config = new XmlConfigBuilder().build();
			config.setSuperClient(true);
			
		} else {
			URL url = ConfigHelper.locateConfig(configResourcePath);
			try {
				config = new UrlXmlConfig(url);
				config.setSuperClient(useSuperClient);
			} catch (IOException e) {
				throw new CacheException(e);
			}
		}
		
		return Hazelcast.newHazelcastInstance(config);
	}

}
