/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * ChildFirstClassLoader is a classloader which prefers own classes before parent's classes.
 * <p>
 * It simply tries to load class (or a resource) from specified set of URLs and if it cannot find it, it delegates to
 * it's parent.
 */
public class ChildFirstClassLoader extends URLClassLoader {

    private static final ILogger LOG = Logger.getLogger(ChildFirstClassLoader.class);

    private volatile boolean closed;

    public ChildFirstClassLoader(@Nonnull URL[] urls, @Nonnull ClassLoader parent) {
        super(urls, parent);

        for (URL url : urls) {
            try {
                if (!new File(url.toURI()).exists()) {
                    LOG.warning("URL '" + url + "' does not point to an existing local file.");
                }
            } catch (URISyntaxException e) {
                throw new HazelcastException("URL has incorrect syntax", e);
            }
        }

        if (urls.length == 0) {
            throw new IllegalArgumentException("urls must not be null nor empty");
        }
        if (parent == null) {
            throw new IllegalArgumentException("parent must not be null");
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return super.findClass(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // has the class loaded already?
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
            try {
                // find the class from given jar urls as in first constructor parameter.
                loadedClass = findClass(name);
            } catch (ClassNotFoundException ignored) {
                // ignore class not found
            }

            if (loadedClass == null) {
                loadedClass = getParent().loadClass(name);
            }

            if (loadedClass == null) {
                throw new ClassNotFoundException("Could not find class " + name + " in classloader nor in parent classloader");
            }
        }

        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<URL> allRes = new LinkedList<>();

        // load resource from this classloader
        Enumeration<URL> thisRes = findResources(name);
        if (thisRes != null) {
            while (thisRes.hasMoreElements()) {
                allRes.add(thisRes.nextElement());
            }
        }

        // then try finding resources from parent classloaders
        Enumeration<URL> parentRes = super.findResources(name);
        if (parentRes != null) {
            while (parentRes.hasMoreElements()) {
                allRes.add(parentRes.nextElement());
            }
        }

        return new Enumeration<URL>() {
            final Iterator<URL> it = allRes.iterator();

            @Override
            public boolean hasMoreElements() {
                return it.hasNext();
            }

            @Override
            public URL nextElement() {
                return it.next();
            }
        };
    }

    @Override
    public URL getResource(String name) {
        URL res = findResource(name);
        if (res == null) {
            res = super.getResource(name);
        }
        return res;
    }

    @Override
    public void close() throws IOException {
        super.close();
        closed = true;
    }

    /**
     * Returns if this classloader has been already closed.
     * <p>
     * Visible for testing because there is no easy way to find out if
     * {@link URLClassLoader} has been closed.
     */
    public boolean isClosed() {
        return closed;
    }
}
