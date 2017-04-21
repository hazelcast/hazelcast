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

package com.hazelcast.jet.config;

import com.hazelcast.jet.impl.deployment.ResourceKind;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Job-specific configuration options.
 */
public class JobConfig implements Serializable {

    private final Set<ResourceConfig> resourceConfigs = new HashSet<>();

    /**
     * Adds the supplied classes to the list of resources that will be
     * available on the job's classpath while it's executing in the Jet
     * cluster.
     */
    public JobConfig addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            resourceConfigs.add(new ResourceConfig(clazz));
        }
        return this;
    }

    /**
     * Adds the JAR identified by the supplied URL to the list of JARs that
     * will be a part of the job's classpath while it's executing in the Jet
     * cluster. The JAR filename will be used as the ID of the resource.
     */
    public JobConfig addJar(URL url) {
        return addJar(url, toFilename(url));
    }

    /**
     * Adds the JAR identified by the supplied URL to the list of JARs that
     * will be a part of the job's classpath while it's executing in the Jet
     * cluster. The JAR will be registered under the supplied resource ID.
     */
    public JobConfig addJar(URL url, String resourceId) {
        return add(url, resourceId, ResourceKind.JAR);
    }

    /**
     * Adds the supplied JAR file to the list of JARs that will be a part of
     * the job's classpath while it's executing in the Jet cluster. The JAR
     * filename will be used as the ID of the resource.
     */
    public JobConfig addJar(File file) {
        try {
            return addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied JAR file to the list of JARs that will be a part of
     * the job's classpath while it's executing in the Jet cluster. The JAR
     * will be registered under the supplied resource ID.
     */
    public JobConfig addJar(File file, String id) {
        try {
            return addJar(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the JAR identified by the supplied pathname to the list of JARs
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster. The JAR filename will be used as the ID of the resource.
     */
    public JobConfig addJar(String path) {
        try {
            File file = new File(path);
            return addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the JAR identified by the supplied pathname to the list of JARs
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster. The JAR will be registered under the supplied resource ID.
     */
    public JobConfig addJar(String path, String id) {
        try {
            return addJar(new File(path).toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(URL url) {
        return addResource(url, toFilename(url));
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(URL url, String id) {
        return add(url, id, ResourceKind.DATA);
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource's
     * filename will be used as its ID.
     */
    public JobConfig addResource(File file) {
        try {
            return addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource
     * will be registered under the supplied ID.
     */
    public JobConfig addResource(File file, String id) {
        try {
            return add(file.toURI().toURL(), id, ResourceKind.DATA);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(String path) {
        File file = new File(path);
        try {
            return addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(String path, String id) {
        try {
            return addResource(new File(path).toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Returns all the registered resource configurations.
     */
    public Set<ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private JobConfig add(URL url, String id, ResourceKind type) {
        resourceConfigs.add(new ResourceConfig(url, id, type));
        return this;
    }

    private static String toFilename(URL url) {
        String urlFile = url.getFile();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }
}
