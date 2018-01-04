/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.spi.annotation.Beta;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of User Code Deployment.
 * When enabled client sends configured classes to cluster.
 * This simplifies deployment as you do not have to deploy your domain classes into classpath of all
 * cluster members.
 */
@Beta
public class ClientUserCodeDeploymentConfig {

    private boolean enabled;
    private final List<String> classNames = new ArrayList<String>();
    private final List<String> jarPaths = new ArrayList<String>();

    /**
     * @return {{@code true}} when User Code Deployment is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled Enable or disable User Code Deployment. Default: {{@code false}}
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * @return list of configured class names
     */
    public List<String> getClassNames() {
        return classNames;
    }

    /**
     * @return list of configured jar path
     */
    public List<String> getJarPaths() {
        return jarPaths;
    }

    /**
     * @param classNames names of the classes that will be send to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig setClassNames(List<String> classNames) {
        this.classNames.clear();
        this.classNames.addAll(classNames);
        return this;
    }

    /**
     * String jarPath is searched in following order:
     * 1. as absolute path,
     * 2. as URL,
     * 3. and in classpath.
     *
     * @param jarPaths add list of jarPaths that will be send to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig setJarPaths(List<String> jarPaths) {
        this.jarPaths.clear();
        this.jarPaths.addAll(jarPaths);
        return this;
    }

    /**
     * @param className name of the class that will be send to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addClass(String className) {
        classNames.add(className);
        return this;
    }

    /**
     * @param clazz class that will be send to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addClass(Class clazz) {
        classNames.add(clazz.getName());
        return this;
    }

    /**
     * String jarPath is searched in following order:
     * 1. as absolute path,
     * 2. as URL,
     * 3. and in classpath.
     *
     * @param jarPath path of the jar that will be send to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addJar(String jarPath) {
        jarPaths.add(jarPath);
        return this;
    }

    /**
     * @param jarFile path of the jar that will be send to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addJar(File jarFile) {
        jarPaths.add(jarFile.getAbsolutePath());
        return this;
    }

}
