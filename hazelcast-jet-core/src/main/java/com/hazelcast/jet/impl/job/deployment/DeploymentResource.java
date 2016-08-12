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

package com.hazelcast.jet.impl.job.deployment;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.JarInputStream;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DeploymentResource {
    private static final int CLASS_PREFIX = 0xcafebabe;
    private final transient InputStream inputStream;
    private final ResourceDescriptor descriptor;

    public DeploymentResource(URL url, String name, ResourceType type) throws IOException {
        this.descriptor = new ResourceDescriptor(name, type);
        this.inputStream = url.openStream();
    }

    public DeploymentResource(Class clazz) throws IOException {
        ProtectionDomain protectionDomain = clazz.getProtectionDomain();
        String classAsPath = clazz.getName().replace('.', '/') + ".class";
        ResourceType resourceType = null;

        URL location = getLocation(protectionDomain);

        if (location != null) {
            resourceType = getContentType(location);
        }

        if (location == null) {
            this.inputStream = clazz.getClassLoader().getResourceAsStream(classAsPath);
            this.descriptor = new ResourceDescriptor(clazz.getName(), ResourceType.CLASS);
        } else {
            if (resourceType != ResourceType.JAR) {
                throw new IllegalStateException("Something wrong with class=" + clazz + "it should be a part of jar archive");
            }

            this.inputStream = location.openStream();
            this.descriptor = new ResourceDescriptor(location.toString(), ResourceType.JAR);
        }
        checkNotNull(this.descriptor, "Descriptor is null");
        checkNotNull(this.inputStream, "InputStream is null");
    }


    public InputStream getInputStream() {
        return inputStream;
    }

    public ResourceDescriptor getDescriptor() {
        return descriptor;
    }

    private ResourceType getContentType(URL url) throws IOException {

        try (DataInputStream in = new DataInputStream(url.openStream())) {
            int magic = in.readInt();
            // Check that is it class
            if (magic == CLASS_PREFIX) {
                return ResourceType.CLASS;
            }
        }

        try (JarInputStream jarInputStream = new JarInputStream(url.openStream())) {
            if (jarInputStream.getNextJarEntry() != null) {
                return ResourceType.JAR;
            }
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return ResourceType.DATA;
        }

        return ResourceType.DATA;
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
