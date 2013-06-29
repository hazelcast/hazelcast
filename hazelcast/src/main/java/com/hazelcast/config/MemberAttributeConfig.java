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

package com.hazelcast.config;

import java.util.HashMap;
import java.util.Map;

public class MemberAttributeConfig {

	private final Map<String, Object> attributes = new HashMap<String, Object>();
	
	public Map<String, Object> getAttributes() {
		return attributes;
	}
	
	public void setAttribute(String key, Object value) {
		attributes.put(key, value);
	}
	
	public void removeAttribute(String key) {
		attributes.remove(key);
	}
	
	public Object getAttribute(String key) {
		return attributes.get(key);
	}
	
}
