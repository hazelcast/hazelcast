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
 * Job specific configuration options
 */
public class JobConfig implements Serializable {

    private static final int DEFAULT_RESOURCE_PART_SIZE = 1 << 14;
    private final Set<ResourceConfig> resourceConfigs = new HashSet<>();

    /**
     * Chunk size for resources when transmitting them over the wire
     */
    public int getResourcePartSize() {
        return DEFAULT_RESOURCE_PART_SIZE;
    }

    /**
     * The given classes will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     */
    public JobConfig addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            resourceConfigs.add(new ResourceConfig(clazz));
        }
        return this;
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
     */
    public JobConfig addJar(URL url) {
        return addJar(url, getFileName(url));
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the JAR file.
     */
    public JobConfig addJar(URL url, String id) {
        return add(url, id, ResourceKind.JAR);
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
     */
    public void addJar(File file) {
        try {
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the JAR file.
     */
    public void addJar(File file, String id) {
        try {
            addJar(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
     */
    public void addJar(String path) {
        try {
            File file = new File(path);
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * The given JAR file will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the JAR file.
     */
    public void addJar(String path, String id) {
        try {
            addJar(new File(path).toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }


    /**
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
     */
    public JobConfig addResource(URL url) {
        return addResource(url, getFileName(url));
    }

    /**
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the resource file.
     */
    public JobConfig addResource(URL url, String id) {
        return add(url, id, ResourceKind.DATA);
    }

    /**
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
     */
    public JobConfig addResource(File file) {
        try {
            return addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }

    }

    /**
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the resource file.
     */
    public JobConfig addResource(File file, String id) {
        try {
            return add(file.toURI().toURL(), id, ResourceKind.DATA);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The filename will be used as the ID of the resource.
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
     * The given resource will be deployed to all the nodes before job execution
     * and available for the job specific class loader.
     *
     * The provided ID will be assigned to the resource file.
     */
    public JobConfig addResource(String path, String id) {
        File file = new File(path);
        try {
            return addResource(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Returns all the resource configurations
     *
     * @return resource configuration set
     */
    public Set<ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private JobConfig add(URL url, String id, ResourceKind type) {
        resourceConfigs.add(new ResourceConfig(url, id, type));
        return this;
    }

    private String getFileName(URL url) {
        String urlFile = url.getFile();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }

}
