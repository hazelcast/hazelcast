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

package com.hazelcast.config;

import java.util.Properties;

import com.hazelcast.security.IPermissionPolicy;

public class PermissionPolicyConfig {
	
	private String className = null;
	
	private IPermissionPolicy policyImpl = null;
	
	private Properties properties = new Properties();

	public PermissionPolicyConfig() {
		super();
	}
	
	public PermissionPolicyConfig(String className) {
		super();
		this.className = className;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public IPermissionPolicy getPolicyImpl() {
		return policyImpl;
	}

	public void setPolicyImpl(IPermissionPolicy policyImpl) {
		this.policyImpl = policyImpl;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
}
