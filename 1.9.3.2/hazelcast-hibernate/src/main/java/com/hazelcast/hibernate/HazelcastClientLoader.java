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
import java.util.logging.Level;

import org.hibernate.cache.CacheException;
import org.hibernate.util.PropertiesHelper;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

class HazelcastClientLoader implements IHazelcastInstanceLoader {
	
	private final static ILogger logger = Logger.getLogger(HazelcastInstanceFactory.class.getName());

	public HazelcastInstance loadInstance(Properties props) throws CacheException {
		if(props == null) {
			throw new NullPointerException("Hibernate environment properties is null!");
		}
		
		String[] hosts = PropertiesHelper.toStringArray(CacheEnvironment.NATIVE_CLIENT_HOSTS, ",", props);
    	String group = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_GROUP, props, null);
    	String pass = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_PASSWORD, props, null);
    	
    	if(hosts == null || hosts.length == 0 || group == null || pass == null) {
    		throw new CacheException("Configuration properties " + CacheEnvironment.NATIVE_CLIENT_HOSTS + ", " 
    				+ CacheEnvironment.NATIVE_CLIENT_GROUP + " and " + CacheEnvironment.NATIVE_CLIENT_PASSWORD 
    				+ " are mandatory to use native client!");
    	}
    	
    	StringBuilder msg = new StringBuilder("Creating HazelcastClient for group: '");
    	msg.append(group).append("' via hosts: '");
    	
    	for (int i = 0; i < hosts.length; i++) {
			msg.append(hosts[i]).append("'").append(", ");
		}
    	msg.deleteCharAt(msg.length() -1).append(".");
    	
    	logger.log(Level.INFO, msg.toString());
    	
    	return HazelcastClient.newHazelcastClient(group, pass, hosts);
	}
}
