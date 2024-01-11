/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.config.UserCodeNamespaceConfig;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

/**
 * A ClassLoader that's aware of the configured namespaces and their resources.
 * The classloading scheme does not follow the recommended {@link ClassLoader} parent delegation model: instead, this
 * {@code ClassLoader} first looks up classes and resources on its own, then delegates if not found.
 *
 * @see UserCodeNamespaceConfig
 */
public class NamespaceAwareClassLoader extends ClassLoader {
    // Retain Parent for faster referencing (skips permission checks)
    private final ClassLoader parent;

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public NamespaceAwareClassLoader(ClassLoader parent) {
        super(parent);
        this.parent = parent;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            ClassLoader candidate = pickClassLoader();
            Class<?> klass = candidate.loadClass(name);
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }

    ClassLoader pickClassLoader() {
        ClassLoader classLoader = NamespaceThreadLocalContext.getClassLoader();
        if (classLoader != null) {
            return classLoader;
        }
        return parent;
    }

    @Override
    public URL getResource(String name) {
        return pickClassLoader().getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return pickClassLoader().getResources(name);
    }
}
