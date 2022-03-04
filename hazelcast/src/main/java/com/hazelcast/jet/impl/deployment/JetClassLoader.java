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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class JetClassLoader extends JetDelegatingClassLoader {

    private static final String JOB_URL_PROTOCOL = "jet-job-resource";

    private final long jobId;
    private final String jobName;
    private final Supplier<IMap<String, byte[]>> resourcesSupplier;
    private final ILogger logger;
    private final JobResourceURLStreamHandler jobResourceURLStreamHandler;

    private volatile boolean isShutdown;

    public JetClassLoader(
            @Nonnull NodeEngine nodeEngine,
            @Nullable ClassLoader parent,
            @Nullable String jobName,
            long jobId,
            @Nonnull JobRepository jobRepository
    ) {
        super(parent);
        this.jobName = jobName;
        this.jobId = jobId;
        this.resourcesSupplier = Util.memoizeConcurrent(() -> jobRepository.getJobResources(jobId));
        this.logger = nodeEngine.getLogger(getClass());
        this.jobResourceURLStreamHandler = new JobResourceURLStreamHandler();
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(toClassResourceId(name));
        if (classBytesStream == null) {
            throw new ClassNotFoundException(name + ". Add it using " + JobConfig.class.getSimpleName()
                    + " or start all members with it on classpath");
        }
        byte[] classBytes = uncheckCall(() -> IOUtil.toByteArray(classBytesStream));
        definePackage(name);
        return defineClass(name, classBytes, 0, classBytes.length);
    }

    /**
     * Defines the package if it is not already defined for the given class
     * name.
     *
     * @param className the class name
     */
    private void definePackage(String className) {
        if (isEmpty(className)) {
            return;
        }
        int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return;
        }
        String packageName = className.substring(0, lastDotIndex);
        if (getPackage(packageName) != null) {
            return;
        }
        try {
            definePackage(packageName, null, null, null, null, null, null, null);
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Override
    protected URL findResource(String name) {
        if (checkShutdown(name) || isEmpty(name) || !resourcesSupplier.get().containsKey(classKeyName(name))) {
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

    public void shutdown() {
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    private InputStream resourceStream(String name) {
        if (checkShutdown(name)) {
            return null;
        }
        byte[] classData = resourcesSupplier.get().get(classKeyName(name));
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    private boolean checkShutdown(String resource) {
        if (!isShutdown) {
            return false;
        }
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
                ", jobId=" + idToString(jobId) +
                '}';
    }
}
