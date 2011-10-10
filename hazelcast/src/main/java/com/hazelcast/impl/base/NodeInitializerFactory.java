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

package com.hazelcast.impl.base;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.AbstractSerializer;

public final class NodeInitializerFactory {
	
	private static final ILogger logger = Logger.getLogger(NodeInitializerFactory.class.getName());
	private static final String FACTORY_ID = "com.hazelcast.NodeInitializer";
	
	public static NodeInitializer create() {
		NodeInitializer initializer = null;
		final String initializerClassname = ServiceLoader.load(FACTORY_ID);
		if(initializerClassname != null) {
			try {
				Class klass = AbstractSerializer.loadClass(initializerClassname);
				initializer = (NodeInitializer) AbstractSerializer.newInstance(klass);
			} catch (Exception e) {
				logger.log(Level.WARNING, "Initializer instance of class[" + initializerClassname + "] " +
						"could not be instantiated! => " 
						+ e.getClass().getName() + ": " + e.getMessage());
			}
		}
        return initializer != null ? initializer : createDefault();
	}
	
	public static NodeInitializer createDefault() {
		return new DefaultNodeInitializer();
	}
	
	
	/**
	 * ServiceLoader utility similar to Java 6 ServiceLoader
	 */
	private static class ServiceLoader {
		public static String load(String factoryId) {
			final ClassLoader cl = Thread.currentThread().getContextClassLoader();
			final String resourceName = "META-INF/services/" + factoryId;
			try {
				final Enumeration<URL> configs;
				if(cl != null) {
					configs = cl.getResources(resourceName);
				} else {
					configs = ClassLoader.getSystemResources(resourceName);
				}
				while (configs.hasMoreElements()) {
					URL url = configs.nextElement();
					InputStream in = url.openStream();
					try {
						BufferedReader r = new BufferedReader(new InputStreamReader(in, "UTF-8"));
						while (true) {
							String line = r.readLine();
							if (line == null) {
								break;
							}
							int comment = line.indexOf('#');
							if (comment >= 0) {
								line = line.substring(0, comment);
							}
							String name = line.trim();
							if (name.length() == 0) {
								continue;
							}

							return name;
						}
					} finally {
						in.close();
					}
				}
			} catch (Exception e) {
				logger.log(Level.WARNING, e.getMessage(), e);
			}
			return null;
		}
	}
	
}
