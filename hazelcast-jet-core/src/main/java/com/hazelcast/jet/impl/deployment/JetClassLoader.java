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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.NodeEngine;

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

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class JetClassLoader extends ClassLoader {

    private static final String JOB_URL_PROTOCOL = "jet-job-resource";

    private final long jobId;
    private final String jobName;
    private final Map<String, byte[]> resources;
    private final ILogger logger;
    private final JobResourceURLStreamHandler jobResourceURLStreamHandler;

    private volatile boolean isShutdown;

    public JetClassLoader(NodeEngine nodeEngine,
                          @Nullable ClassLoader parent, @Nullable String jobName,
                          long jobId, Map<String, byte[]> resources) {
        super(parent == null ? JetClassLoader.class.getClassLoader() : parent);
        this.jobName = jobName;
        this.jobId = jobId;
        this.resources = resources;
        this.logger = nodeEngine.getLogger(getClass());
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
        if (!checkShutdown(name)) {
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
        return null;
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        return new SingleURLEnumeration(findResource(name));
    }

    public void shutdown() {
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    @SuppressWarnings("unchecked")
    private InputStream resourceStream(String name) {
        if (!checkShutdown(name)) {
            byte[] classData = resources.get(name);
            if (classData == null) {
                return null;
            }
            return new InflaterInputStream(new ByteArrayInputStream(classData));
        }
        return null;
    }

    private boolean checkShutdown(String resource) {
        if (isShutdown) {
            // This class loader is used as the thread context CL in several places. It's possible
            // that another thread inherits this classloader since a Thread inherits the parent's
            // context CL by default (see for example: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8172726)
            // In these scenarios the thread might essentially hold a reference to an obsolete classloader.
            // Rather than throwing an unexpected exception we instead print a warning.
            String jobName = this.jobName == null ? idToString(jobId) : "'" + this.jobName + "'";
            logger.warning("Classloader for job " + jobName + " tried to load '" + resource
                    + "' after the job was completed. The classloader used for jobs is disposed after " +
                    "job is completed");
            return true;
        }
        return false;
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

    @Override
    public String toString() {
        return "JetClassLoader{" +
                "jobName='" + jobName + '\'' +
                ", jobId=" + jobId +
                '}';
    }
}
