/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

public class Build {

	public static final boolean DEBUG;

	public String build = "do-not-change";

	public String version = "do-not-change";

	static {
		if ("true".equals(System.getProperty("hazelcast.debug"))) { 
			DEBUG = true;
		} else {
			DEBUG = false;
		}
	}

	private Build() {
		// IMPORTANT: DO NOT REMOVE THE FOLLOWING LINES
		// THEY ARE USED AT BUILD TIME
		// @build
		// @version
		// =============================================
	}

	private static final Build instance = new Build();

	public static Build get() {
		return instance;
	}
}
