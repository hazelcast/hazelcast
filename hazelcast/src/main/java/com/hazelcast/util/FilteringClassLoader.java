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

import com.hazelcast.nio.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is used to separate Server and Client inside the same JVM on new
 * standalone client unittests!<br/>
 * NEVER EVER use this anywhere in production! :D
 */
public class FilteringClassLoader
        extends ClassLoader {

    private static final int BUFFER_SIZE = 1024;

    private final Map<String, Class<?>> cache = new ConcurrentHashMap<String, Class<?>>();

    private final List<String> excludePackages;
    private final ClassLoader delegatingClassLoader;
    private final String enforcedSelfLoadingPackage;

    public FilteringClassLoader(List<String> excludePackages, String enforcedSelfLoadingPackage) {
        this.excludePackages = Collections.unmodifiableList(excludePackages);
        this.enforcedSelfLoadingPackage = enforcedSelfLoadingPackage;

        try {
            Field parent = ClassLoader.class.getDeclaredField("parent");
            parent.setAccessible(true);

            delegatingClassLoader = (ClassLoader) parent.get(this);
            parent.set(this, null);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public URL getResource(String name) {
        return delegatingClassLoader.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException {

        return delegatingClassLoader.getResources(name);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        return delegatingClassLoader.getResourceAsStream(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {

        ValidationUtil.isNotNull(name, "name");

        for (String excludePackage : excludePackages) {
            if (name.startsWith(excludePackage)) {
                throw new ClassNotFoundException(name + " - Package excluded explicitly!");
            }
        }

        if (enforcedSelfLoadingPackage != null && name.startsWith(enforcedSelfLoadingPackage)) {
            Class<?> clazz = cache.get(name);
            if (clazz != null) {
                return clazz;
            }

            InputStream is = getResourceAsStream(name.replaceAll("\\.", "/").concat(".class"));
            try {
                byte[] temp = new byte[BUFFER_SIZE];
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                int length;
                while ((length = is.read(temp)) != -1) {
                    baos.write(temp, 0, length);
                }

                byte[] data = baos.toByteArray();
                clazz = defineClass(name, data, 0, data.length);
                cache.put(name, clazz);
                return clazz;

            } catch (Exception e) {
                throw new ClassNotFoundException(name, e);
            } finally {
                IOUtil.closeResource(is);
            }
        }

        return delegatingClassLoader.loadClass(name);
    }
}
