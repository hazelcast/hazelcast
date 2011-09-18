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

import org.hibernate.cache.CacheException;
import org.hibernate.util.PropertiesHelper;

import com.hazelcast.core.HazelcastInstance;

public final class HazelcastInstanceFactory {
	
	private final static String HZ_CLIENT_LOADER_CLASSNAME = "com.hazelcast.hibernate.HazelcastClientLoader";
	private final static String HZ_INSTANCE_LOADER_CLASSNAME = "com.hazelcast.hibernate.HazelcastInstanceLoader";
	
	public static HazelcastInstance createInstance(Properties props) throws CacheException {
		boolean useNativeClient = false;
		
        if(props != null) {
        	useNativeClient = PropertiesHelper.getBoolean(CacheEnvironment.USE_NATIVE_CLIENT, props, false);
        }
        
        IHazelcastInstanceLoader loader = null;
        Class loaderClass = null;
        ClassLoader cl = HazelcastInstanceFactory.class.getClassLoader();
        
        try {
        	if(useNativeClient) {
        		loaderClass = cl.loadClass(HZ_CLIENT_LOADER_CLASSNAME); 
        	}
        	else {
        		loaderClass = cl.loadClass(HZ_INSTANCE_LOADER_CLASSNAME);
        	}
        	loader = (IHazelcastInstanceLoader) loaderClass.newInstance();
        	
		} catch (Exception e) {
			throw new CacheException(e);
		}
        
        return loader.loadInstance(props);
	}
}
