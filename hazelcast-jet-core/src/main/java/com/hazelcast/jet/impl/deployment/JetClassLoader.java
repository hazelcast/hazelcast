/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.nio.IOUtil;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Enumeration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class JetClassLoader extends ClassLoader {

    private static final String JOB_URL_PROTOCOL = "jet-job-resource";

    private final Map<String, byte[]> resources;
    private JobResourceURLStreamHandler jobResourceURLStreamHandler;

    public JetClassLoader(@Nullable ClassLoader parent, Map<String, byte[]> resources) {
        super(parent == null ? JetClassLoader.class.getClassLoader() : parent);
        this.resources = resources;

        jobResourceURLStreamHandler = new JobResourceURLStreamHandler();
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(name.replace('.', '/') + ".class");
        if (classBytesStream == null) {
            throw new ClassNotFoundException(name + ". Add it using " + JobConfig.class.getSimpleName()
                    + " or start all members with it on classpath");
        }
        byte[] classBytes = uncheckCall(() -> IOUtil.toByteArray(classBytesStream));
        return defineClass(name, classBytes, 0, classBytes.length);
    }

    @Override
    protected URL findResource(String name) {
        if (isEmpty(name) || !resources.containsKey(name)) {
            return null;
        }

        try {
            return new URL(JOB_URL_PROTOCOL, null, -1, name, jobResourceURLStreamHandler);
        } catch (MalformedURLException e) {
            // this should never happen with custom URLStreamHandler
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        return new SingleURLEnumeration(findResource(name));
    }

    @SuppressWarnings("unchecked")
    private InputStream resourceStream(String name) {
        byte[] classData = resources.get(name);
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    private static boolean isEmpty(String className) {
        return className == null || className.isEmpty();
    }

    private final class JobResourceURLStreamHandler extends URLStreamHandler {

        @Override
        protected URLConnection openConnection(URL url) {
            return new JobResourceURLConnection(url);
        }
    }

    private final class JobResourceURLConnection extends URLConnection {

        private JobResourceURLConnection(URL url) {
            super(url);
        }

        @Override
        public void connect() {
        }

        @Override
        public InputStream getInputStream() {
            return resourceStream(url.getFile());
        }
    }

    private static final class SingleURLEnumeration implements Enumeration<URL> {

        private URL url;

        private SingleURLEnumeration(URL url) {
            this.url = url;
        }

        @Override
        public boolean hasMoreElements() {
            return url != null;
        }

        @Override
        public URL nextElement() {
            if (url == null) {
                throw new NoSuchElementException();
            }
            try {
                return url;
            } finally {
                url = null;
            }
        }
    }

}
