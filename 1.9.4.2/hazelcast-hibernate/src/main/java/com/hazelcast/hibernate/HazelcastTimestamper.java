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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;

public final class HazelcastTimestamper {
	
	// one hour in ms
	private final static int DEFAULT_TIMEOUT = (3600 * 1000);
	
	public static long nextTimestamp(HazelcastInstance instance) {
		return instance.getCluster().getClusterTime() ; // System time in ms.
	}

	@Deprecated
	public static int getTimeout() {
        return (60 * 1000); // 60 seconds in ms.
    }
	
	public static int getTimeout(HazelcastInstance instance, String regionName) {
		int ttl = DEFAULT_TIMEOUT;
		try {
			final MapConfig cfg = instance.getConfig().findMatchingMapConfig(regionName);
			if(cfg.getTimeToLiveSeconds() > 0) {
				ttl = cfg.getTimeToLiveSeconds() * 1000; // TTL in ms.
			}
		} catch (UnsupportedOperationException ignore) {
			// HazelcastInstance is instance of HazelcastClient, its getConfig() method is not implemented.
			// We are not checking this by using 'instanceof', 
			// because HazelcastClient class may not be known by classloader.
		}
		return ttl;
	}
}
