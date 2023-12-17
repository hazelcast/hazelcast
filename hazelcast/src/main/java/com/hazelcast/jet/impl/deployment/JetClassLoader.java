/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobRepository.classKeyName;

public class JetClassLoader extends MapResourceClassLoader {

    private static final String JOB_URL_PROTOCOL = "jet-job-resource";

    private final long jobId;
    private final String jobName;
    private final ILogger logger;

    public JetClassLoader(
            @Nonnull ILogger logger,
            @Nullable ClassLoader parent,
            @Nullable String jobName,
            long jobId,
            @Nonnull JobRepository jobRepository
    ) {
        super(parent, () -> jobRepository.getJobResources(jobId), false);
        this.jobName = jobName;
        this.jobId = jobId;
        this.logger = logger;
    }

    @Override
    boolean checkShutdown(String resource) {
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

    @Override
    protected URL findResource(String name) {
        if (checkShutdown(name) || isNullOrEmpty(name) || !resourcesSupplier.get().containsKey(classKeyName(name))) {
            return null;
        }
        try {
            return new URL(JOB_URL_PROTOCOL, null, -1, name, new MapResourceURLStreamHandler());
        } catch (MalformedURLException e) {
            // this should never happen with custom URLStreamHandler
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        URL resourceUrl = findResource(name);
        return resourceUrl == null ? Collections.emptyEnumeration()
                : Collections.enumeration(List.of(resourceUrl));
    }

    @Override
    ClassNotFoundException newClassNotFoundException(String name) {
        return new ClassNotFoundException(name + ". Add it using " + JobConfig.class.getSimpleName()
                + " or start all members with it on classpath");
    }

    @Override
    public String toString() {
        return "JetClassLoader{" +
                "jobName='" + jobName + '\'' +
                ", jobId=" + idToString(jobId) +
                '}';
    }
}
