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

package com.hazelcast.jet.impl.application.localization.classloader;

import com.hazelcast.jet.impl.application.localization.JetClassLoaderException;
import com.hazelcast.jet.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.jet.impl.util.JetUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class LocalizationClassLoader extends ClassLoader implements ProxyClassLoader {
    private final Map<String, Class> classes = new HashMap<String, Class>();


    private final Map<String, ClassLoaderEntry> jarEntries = new HashMap<String, ClassLoaderEntry>();
    private final Map<String, ClassLoaderEntry> dataEntries = new HashMap<String, ClassLoaderEntry>();
    private final Map<String, ClassLoaderEntry> classEntries = new HashMap<String, ClassLoaderEntry>();

    public LocalizationClassLoader(Map<LocalizationResourceDescriptor, ResourceStream> resources) {
        for (Map.Entry<LocalizationResourceDescriptor, ResourceStream> entry : resources.entrySet()) {
            LocalizationResourceDescriptor descriptor = entry.getKey();
            switch (descriptor.getResourceType()) {
                case JAR:
                    loadJarStream(entry.getValue());
                    break;
                case CLASS:
                    loadClassStream(descriptor, entry.getValue());
                    break;
                case DATA:
                    loadDataStream(descriptor, entry.getValue());
                    break;
                default:
            }
        }
    }

    private void loadClassStream(LocalizationResourceDescriptor descriptor, ResourceStream resourceStream) {
        byte[] classBytes = JetUtil.readFully(resourceStream.getInputStream());
        this.classEntries.put(descriptor.getName(), new ClassLoaderEntry(classBytes, resourceStream.getBaseUrl()));
    }

    private void loadDataStream(LocalizationResourceDescriptor descriptor, ResourceStream resourceStream) {
        byte[] bytes = JetUtil.readFully(resourceStream.getInputStream());
        this.dataEntries.put(descriptor.getName(), new ClassLoaderEntry(bytes, resourceStream.getBaseUrl()));
    }

    private void loadJarStream(ResourceStream resourceStream) {
        BufferedInputStream bis = null;
        JarInputStream jis = null;

        try {
            bis = new BufferedInputStream(resourceStream.getInputStream());
            jis = new JarInputStream(bis);

            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }

                byte[] b = new byte[JetUtil.KILOBYTE];
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int len;
                byte[] bytes;

                try {
                    while ((len = jis.read(b)) > 0) {
                        out.write(b, 0, len);
                    }

                    bytes = out.toByteArray();
                } finally {
                    out.close();
                }

                this.jarEntries.put(
                        jarEntry.getName(),
                        new ClassLoaderEntry(
                                bytes, "jar:" + resourceStream.getBaseUrl() + "!/" + jarEntry.getName()
                        )
                );
            }
        } catch (IOException e) {
            throw new JetClassLoaderException(e);
        } finally {
            JetUtil.close(bis, jis);
        }
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) {
        Class result;
        byte[] classBytes;

        result = this.classes.get(className);

        if (result != null) {
            return result;
        }

        classBytes = classBytes(className);

        if (classBytes == null) {
            return null;
        }

        result = defineClass(className, classBytes, 0, classBytes.length);

        if (result == null) {
            return null;
        }

        if (result.getPackage() == null) {
            int lastDotIndex = className.lastIndexOf('.');
            String packageName = (lastDotIndex >= 0) ? className.substring(0, lastDotIndex) : "";
            definePackage(packageName, null, null, null, null, null, null, null);
        }

        if (resolveIt) {
            resolveClass(result);
        }

        this.classes.put(className, result);
        return result;
    }

    @Override
    public InputStream loadResource(String name) {
        byte[] arr = classBytes(name);

        if (arr == null) {
            ClassLoaderEntry classLoaderEntry = this.dataEntries.get(name);
            if (classLoaderEntry != null) {
                arr = classLoaderEntry.getResourceBytes();
            }
        }

        if (arr != null) {
            return new ByteArrayInputStream(arr);
        }

        return null;
    }

    @Override
    public URL findResource(String name) {
        URL url = getResourceURL(name);

        if (url != null) {
            return url;
        }

        return null;
    }

    private ClassLoaderEntry coalesce(String name, Map<String, ClassLoaderEntry>... resources) {
        for (Map<String, ClassLoaderEntry> map : resources) {
            ClassLoaderEntry entry = map.get(name);
            if (entry != null) {
                return entry;
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private byte[] classBytes(String name) {
        ClassLoaderEntry entry = coalesce(name, this.classEntries, this.jarEntries);

        if (entry != null) {
            return entry.getResourceBytes();
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private URL getResourceURL(String name) {
        ClassLoaderEntry entry = coalesce(
                name, this.classEntries,
                this.dataEntries, this.jarEntries
        );

        if (entry != null) {
            if (entry.getBaseUrl() == null) {
                throw new JetClassLoaderException("non-URL accessible resource");
            }

            try {
                return new URL(entry.getBaseUrl());
            } catch (MalformedURLException e) {
                throw new JetClassLoaderException(e);
            }
        }

        return null;
    }
}
