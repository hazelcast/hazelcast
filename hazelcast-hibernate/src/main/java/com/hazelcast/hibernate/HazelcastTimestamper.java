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

import com.hazelcast.core.HazelcastInstance;

public final class HazelcastTimestamper {
	
	private final static int TRUNC_RATIO = 10;
	
	// 60 seconds
	private final static int TIMEOUT = (60 * 1000) / TRUNC_RATIO;
	
	public static long nextTimestamp(HazelcastInstance instance) {
		return instance.getCluster().getClusterTime() / TRUNC_RATIO;
	}

	public static int getTimeout() {
        return TIMEOUT;
    }
}
