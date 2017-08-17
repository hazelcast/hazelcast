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

package com.hazelcast.util;

import com.hazelcast.internal.usercodedeployment.impl.ClassloadingMutexProvider;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * This is used to separate Server and Client inside the same JVM on new standalone client unit tests.
 *
 * NEVER EVER use this anywhere in production! :D
 */
@PrivateApi
public class FilteringClassLoader extends ClassLoader {

    private static final int BUFFER_SIZE = 1024;

    private final ClassloadingMutexProvider mutexProvider = new ClassloadingMutexProvider();
    private final Pattern pattern = Pattern.compile("\\.");
    private final byte[] buffer = new byte[BUFFER_SIZE];

    private final List<String> excludePackages;
    private final String enforcedSelfLoadingPackage;
    private ClassLoader delegatingClassLoader;

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
    public Enumeration<URL> getResources(String name) throws IOException {
        return delegatingClassLoader.getResources(name);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        return delegatingClassLoader.getResourceAsStream(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        isNotNull(name, "name");

        for (String excludePackage : excludePackages) {
            if (name.startsWith(excludePackage)) {
                throw new ClassNotFoundException(name + " - Package excluded explicitly!");
            }
        }

        if (enforcedSelfLoadingPackage != null && name.startsWith(enforcedSelfLoadingPackage)) {
            Closeable classLoadingMutex = mutexProvider.getMutexForClass(name);
            try {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (classLoadingMutex) {
                    Class<?> clazz = findLoadedClass(name);
                    if (clazz == null) {
                        clazz = loadAndDefineClass(name);
                    }
                    return clazz;
                }
            } finally {
                closeResource(classLoadingMutex);
            }
        }
        return delegatingClassLoader.loadClass(name);
    }

    private Class<?> loadAndDefineClass(String name) throws ClassNotFoundException {
        InputStream is = null;
        ByteArrayOutputStream os = null;
        try {
            is = getResourceAsStream(pattern.matcher(name).replaceAll("/").concat(".class"));
            os = new ByteArrayOutputStream();

            int length;
            while ((length = is.read(buffer)) != -1) {
                os.write(buffer, 0, length);
            }

            byte[] data = os.toByteArray();
            return defineClass(name, data, 0, data.length);
        } catch (Exception e) {
            throw new ClassNotFoundException(name, e);
        } finally {
            closeResource(os);
            closeResource(is);
        }
    }
}
