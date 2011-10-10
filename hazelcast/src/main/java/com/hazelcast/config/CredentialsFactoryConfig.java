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

import com.hazelcast.security.ICredentialsFactory;

public class CredentialsFactoryConfig {
	
	private String className = null;
	
	private ICredentialsFactory factoryImpl = null;
	
	private Properties properties = new Properties();

	public CredentialsFactoryConfig() {
		super();
	}
	
	public CredentialsFactoryConfig(String className) {
		super();
		this.className = className;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public ICredentialsFactory getFactoryImpl() {
		return factoryImpl;
	}

	public void setFactoryImpl(ICredentialsFactory factoryImpl) {
		this.factoryImpl = factoryImpl;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

}
