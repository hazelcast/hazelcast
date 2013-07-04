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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;

/**
 * @author mdogan 10/2/12
 */
public class ServiceLoader {

    private static final ILogger logger = Logger.getLogger(ServiceLoader.class.getName());

    public static <T> T load(Class<T> clazz, String factoryId, ClassLoader classLoader) throws Exception {
        final Iterator<T> iter = iterator(clazz, factoryId, classLoader);
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    public static <T> Iterator<T> iterator(final Class<T> clazz, final String factoryId, final ClassLoader classLoader) throws Exception {
        final Set<ServiceDefinition> classDefinitions = parse(factoryId, classLoader);
        // If we are in a multi classloader environment like JEE we need to ask the hazelcast classloader for default services
        if (classLoader != null && ServiceLoader.class.getClassLoader() != classLoader) {
            classDefinitions.addAll(parse(factoryId, ServiceLoader.class.getClassLoader()));
        }
        return new Iterator<T>() {
            final Iterator<ServiceDefinition> classIter = classDefinitions.iterator();

            public boolean hasNext() {
                return classIter.hasNext();
            }

            public T next() {
                final ServiceDefinition classDefinition = classIter.next();
                try {
                    String className = classDefinition.className;
                    ClassLoader classLoader = classDefinition.classLoader;
                    return clazz.cast(ClassLoaderUtil.newInstance(classLoader, className));
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static Set<ServiceDefinition> parse(String factoryId, ClassLoader classLoader) {
        final ClassLoader cl = (classLoader == null) ? Thread.currentThread().getContextClassLoader() : classLoader;
        final String resourceName = "META-INF/services/" + factoryId;
        try {
            final Enumeration<URL> configs;
            if (cl != null) {
                configs = cl.getResources(resourceName);
            } else {
                configs = ClassLoader.getSystemResources(resourceName);
            }
            final Set<ServiceDefinition> names = new HashSet<ServiceDefinition>();
            while (configs.hasMoreElements()) {
                URL url = configs.nextElement();
                BufferedReader r = null;
                try {
                    r = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
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
                        names.add(new ServiceDefinition(name, classLoader));
                    }
                } finally {
                    IOUtil.closeResource(r);
                }
            }
            return names;
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return Collections.emptySet();
    }

    private static class ServiceDefinition {
        private final String className;
        private final ClassLoader classLoader;

        private ServiceDefinition(String className, ClassLoader classLoader) {
            this.className = className;
            this.classLoader = classLoader;
        }
    }

}
