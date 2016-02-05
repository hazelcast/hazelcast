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

package com.hazelcast.jet.impl.application;

import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.security.CodeSource;
import java.util.jar.JarInputStream;
import java.security.ProtectionDomain;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.application.LocalizationResourceType.JAR;
import static com.hazelcast.jet.impl.application.LocalizationResourceType.DATA;
import static com.hazelcast.jet.impl.application.LocalizationResourceType.CLASS;

public class LocalizationResource {
    public static final int CLASS_PREFIX = 0xcafebabe;
    private final transient InputStream inputStream;
    private final LocalizationResourceDescriptor descriptor;

    public LocalizationResource(Class clazz) throws IOException {
        ProtectionDomain protectionDomain = clazz.getProtectionDomain();
        String classAsPath = clazz.getName().replace('.', '/') + ".class";
        LocalizationResourceType localizationResourceType = null;

        URL location = getLocation(protectionDomain);

        if ((location != null)) {
            localizationResourceType = getContentType(location);
        }

        if ((location == null) || (localizationResourceType == DATA)) {
            this.inputStream = clazz.getClassLoader().getResourceAsStream(classAsPath);
            this.descriptor = new LocalizationResourceDescriptor(clazz.getName(), CLASS);
        } else {
            if (localizationResourceType != JAR) {
                throw new IllegalStateException("Something wrong with class=" + clazz + "it should be a part of jar archive");
            }

            this.inputStream = location.openStream();
            this.descriptor = new LocalizationResourceDescriptor(location.toString(), JAR);
        }

        checkNotNull(this.descriptor, "Descriptor is null");
        checkNotNull(this.inputStream, "InputStream is null");
    }

    public LocalizationResource(URL url) throws IOException {
        checkNotNull(url, "Url is null");
        this.inputStream = url.openStream();

        checkNotNull(this.inputStream, "InputStream is null");
        this.descriptor = new LocalizationResourceDescriptor(url.toString(), getContentType(url));
    }

    public LocalizationResource(InputStream inputStream, String name, LocalizationResourceType resourceType) {
        checkNotNull(inputStream, "InputStream is null");

        this.inputStream = inputStream;
        this.descriptor = new LocalizationResourceDescriptor(name, resourceType);
    }

    private LocalizationResourceType getContentType(URL url) throws IOException {
        DataInputStream in = new DataInputStream(url.openStream());

        try {
            int magic = in.readInt();

            // Check that is it class
            if (magic == CLASS_PREFIX) {
                return CLASS;
            }
        } finally {
            in.close();
        }

        JarInputStream jarInputStream = new JarInputStream(url.openStream());

        try {
            if (jarInputStream.getNextJarEntry() != null) {
                return JAR;
            }
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return DATA;
        } finally {
            jarInputStream.close();
        }

        return DATA;
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

    public InputStream openStream() {
        return this.inputStream;
    }

    public LocalizationResourceDescriptor getDescriptor() {
        return descriptor;
    }
}
