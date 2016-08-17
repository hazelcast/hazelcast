/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.job.deployment.DeploymentDescriptor;
import com.hazelcast.jet.impl.job.deployment.DeploymentType;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents deployment configuration
 */
public class DeploymentConfig implements Serializable {
    private DeploymentDescriptor descriptor;
    private URL url;

    /**
     * @param url  url of the deployment
     * @param id   id of the deployment
     * @param type type of the deployment
     * @throws IOException if IO error happens
     */
    public DeploymentConfig(URL url, String id, DeploymentType type) throws IOException {
        this.descriptor = new DeploymentDescriptor(id, type);
        this.url = url;
    }

    /**
     * @param clazz class file to deploy
     * @throws IOException if IO error happens
     */
    public DeploymentConfig(Class clazz) throws IOException {
        String classAsPath = clazz.getName().replace('.', '/') + ".class";
        this.url = clazz.getClassLoader().getResource(classAsPath);
        checkNotNull(this.url, "URL is null");
        this.descriptor = new DeploymentDescriptor(clazz.getName(), DeploymentType.CLASS);
    }

    /**
     * Returns the URL of the deployment
     *
     * @return URL
     */
    public URL getUrl() {
        return url;
    }

    /**
     * Returns the {@link DeploymentDescriptor} for the deployment
     *
     * @return DeploymentDescriptor
     */
    public DeploymentDescriptor getDescriptor() {
        return descriptor;
    }


}
