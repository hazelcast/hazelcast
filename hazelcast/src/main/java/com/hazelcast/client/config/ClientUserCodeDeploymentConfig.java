/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.UserCodeNamespacesConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Configuration of User Code Deployment.
 * When enabled client sends configured classes to cluster.
 * This simplifies deployment as you do not have to deploy your domain classes into classpath of all
 * cluster members.
 *
 * @deprecated "User Code Deployment" is replaced by the "User Code Namespaces" feature
 * @see UserCodeNamespacesConfig
 */
@Deprecated(since = "5.4", forRemoval = true)
public class ClientUserCodeDeploymentConfig {

    private boolean enabled;
    private final List<String> classNames;
    private final List<String> jarPaths;

    public ClientUserCodeDeploymentConfig() {
        classNames = new ArrayList<>();
        jarPaths = new ArrayList<>();
    }

    public ClientUserCodeDeploymentConfig(ClientUserCodeDeploymentConfig userCodeDeploymentConfig) {
        enabled = userCodeDeploymentConfig.enabled;
        classNames = new ArrayList<>(userCodeDeploymentConfig.classNames);
        jarPaths = new ArrayList<>(userCodeDeploymentConfig.jarPaths);
    }

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
     * @param classNames names of the classes that will be sent to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig setClassNames(List<String> classNames) {
        isNotNull(classNames, "classNames");
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
     * @param jarPaths add list of jarPaths that will be sent to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig setJarPaths(List<String> jarPaths) {
        isNotNull(jarPaths, "jarPaths");
        this.jarPaths.clear();
        this.jarPaths.addAll(jarPaths);
        return this;
    }

    /**
     * @param className name of the class that will be sent to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addClass(String className) {
        classNames.add(className);
        return this;
    }

    /**
     * @param clazz class that will be sent to cluster
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addClass(Class<?> clazz) {
        classNames.add(clazz.getName());
        return this;
    }

    /**
     * String jarPath is searched in following order:
     * 1. as absolute path,
     * 2. as URL,
     * 3. and in classpath.
     *
     * @param jarPath path of the jar that will be sent to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addJar(String jarPath) {
        jarPaths.add(jarPath);
        return this;
    }

    /**
     * @param jarFile path of the jar that will be sent to clusters
     * @return this for chaining
     */
    public ClientUserCodeDeploymentConfig addJar(File jarFile) {
        jarPaths.add(jarFile.getAbsolutePath());
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientUserCodeDeploymentConfig that = (ClientUserCodeDeploymentConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (!classNames.equals(that.classNames)) {
            return false;
        }
        return jarPaths.equals(that.jarPaths);
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + classNames.hashCode();
        result = 31 * result + jarPaths.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClientUserCodeDeploymentConfig{"
                + "enabled=" + enabled
                + ", classNames=" + classNames
                + ", jarPaths=" + jarPaths
                + '}';
    }
}
