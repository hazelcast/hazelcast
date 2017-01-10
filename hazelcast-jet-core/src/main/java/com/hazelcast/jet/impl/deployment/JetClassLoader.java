/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.util.EmptyStatement;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class JetClassLoader extends ClassLoader {

    private final Map<String, Class> classes = new HashMap<>();
    private ResourceStore store;

    public JetClassLoader(ResourceStore store) {
        this.store = store;
    }

    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        return loadClass(className, true);
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
        if (isEmpty(className)) {
            return null;
        }
        synchronized (getClassLoadingLock(className)) {
            final Class cached = classes.get(className);
            if (cached != null) {
                return cached;
            }

            try {
                return getClass().getClassLoader().loadClass(className);
            } catch (ClassNotFoundException ignored) {
                EmptyStatement.ignore(ignored);
            }

            byte[] classBytes = classBytes(className);
            if (classBytes == null) {
                throw new ClassNotFoundException(className);
            }
            final Class defined = defineClass(className, classBytes, 0, classBytes.length);
            if (defined == null) {
                throw new ClassNotFoundException(className);
            }
            if (defined.getPackage() == null) {
                int lastDotIndex = className.lastIndexOf('.');
                if (lastDotIndex >= 0) {
                    String name = className.substring(0, lastDotIndex);
                    definePackage(name, null, null, null, null, null, null, null);
                }
            }
            if (resolveIt) {
                resolveClass(defined);
            }
            classes.put(className, defined);
            return defined;
        }
    }

    @Override
    public URL getResource(String name) {
        if (isEmpty(name)) {
            return null;
        }
        URL url = getResourceURL(name);
        if (url == null) {
            return null;
        }
        return url;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (isEmpty(name)) {
            return null;
        }
        byte[] arr = classBytes(name);
        if (arr == null) {
            ClassLoaderEntry classLoaderEntry = store.getDataEntries().get(name);
            if (classLoaderEntry != null) {
                arr = classLoaderEntry.getResourceBytes();
            }
        }
        if (arr == null) {
            return null;
        }
        return new ByteArrayInputStream(arr);
    }

    @SuppressWarnings("unchecked")
    private byte[] classBytes(String name) {
        ClassLoaderEntry entry = coalesce(name, store.getClassEntries(), store.getJarEntries());
        if (entry == null) {
            return null;
        }
        return entry.getResourceBytes();

    }

    @SafeVarargs
    private final ClassLoaderEntry coalesce(String name, Map<String, ClassLoaderEntry>... resources) {
        for (Map<String, ClassLoaderEntry> map : resources) {
            ClassLoaderEntry entry = map.get(name);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    private URL getResourceURL(String name) {
        ClassLoaderEntry entry = coalesce(name, store.getClassEntries(), store.getDataEntries(), store.getJarEntries());
        if (entry == null) {
            return null;
        }
        if (entry.getBaseUrl() == null) {
            throw new IllegalArgumentException("non-URL accessible resource");
        }

        try {
            return new URL(entry.getBaseUrl());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }

    }


    private boolean isEmpty(String className) {
        return className == null || className.isEmpty();
    }

}
