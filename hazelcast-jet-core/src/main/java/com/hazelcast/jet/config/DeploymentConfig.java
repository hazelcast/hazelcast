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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.JarInputStream;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents deployment configuration
 */
public class DeploymentConfig implements Serializable {
    private static final int CLASS_PREFIX = 0xcafebabe;
    private DeploymentDescriptor descriptor;
    private URL url;

    /**
     * @param url  url of the deployment
     * @param name name of the deployment
     * @param type type of the deployment
     * @throws IOException if IO error happens
     */
    public DeploymentConfig(URL url, String name, DeploymentType type) throws IOException {
        this.descriptor = new DeploymentDescriptor(name, type);
        this.url = url;
    }

    /**
     * @param clazz class file to deploy
     * @throws IOException if IO error happens
     */
    public DeploymentConfig(Class clazz) throws IOException {
        ProtectionDomain protectionDomain = clazz.getProtectionDomain();
        String classAsPath = clazz.getName().replace('.', '/') + ".class";
        DeploymentType deploymentType = null;

        this.url = getLocation(protectionDomain);

        if (this.url != null) {
            deploymentType = getContentType(url);
        }

        if (this.url == null) {
            this.url = clazz.getClassLoader().getResource(classAsPath);
            this.descriptor = new DeploymentDescriptor(clazz.getName(), DeploymentType.CLASS);
        } else {
            if (deploymentType != DeploymentType.JAR) {
                throw new IllegalStateException("Something wrong with class=" + clazz + " it should be a part of jar archive");
            }

            this.descriptor = new DeploymentDescriptor(url.toString(), DeploymentType.JAR);
        }
        checkNotNull(this.descriptor, "Descriptor is null");
        checkNotNull(this.url, "URL is null");
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

    private DeploymentType getContentType(URL url) throws IOException {

        try (DataInputStream in = new DataInputStream(url.openStream())) {
            int magic = in.readInt();
            // Check that is it class
            if (magic == CLASS_PREFIX) {
                return DeploymentType.CLASS;
            }
        }

        try (JarInputStream jarInputStream = new JarInputStream(url.openStream())) {
            if (jarInputStream.getNextJarEntry() != null) {
                return DeploymentType.JAR;
            }
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return DeploymentType.DATA;
        }

        return DeploymentType.DATA;
    }

    private URL getLocation(ProtectionDomain protectionDomain) {
        URL location;
        CodeSource codeSource;

        if (protectionDomain != null) {
            codeSource = protectionDomain.getCodeSource();
        } else {
            return null;
        }

        if (codeSource != null) {
            location = codeSource.getLocation();
        } else {
            return null;
        }

        return location;
    }

}
