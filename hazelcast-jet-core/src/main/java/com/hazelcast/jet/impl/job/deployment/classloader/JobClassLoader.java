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

package com.hazelcast.jet.impl.job.deployment.classloader;

import com.hazelcast.jet.impl.job.deployment.DeploymentDescriptor;
import com.hazelcast.jet.impl.job.deployment.DeploymentStorage;
import com.hazelcast.jet.impl.job.deployment.JetClassLoaderException;
import com.hazelcast.jet.impl.util.JetUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class JobClassLoader extends ClassLoader {
    private final List<ProxyClassLoader> loaders = new ArrayList<>();
    private final ProxyClassLoader systemLoader = new SystemLoader();
    private final ProxyClassLoader parentLoader = new ParentLoader();
    private final ProxyClassLoader currentLoader = new CurrentLoader();


    public JobClassLoader(DeploymentStorage storage) {
        addDefaultLoaders();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                addLoader(new JobUserClassLoader(storage.getResources()));
            } catch (IOException e) {
                throw unchecked(e);
            }
            return null;
        });
    }

    private void addDefaultLoaders() {
        addLoader(systemLoader);
        addLoader(parentLoader);
        addLoader(currentLoader);
    }

    private void addLoader(ProxyClassLoader loader) {
        loaders.add(loader);
    }

    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        return loadClass(className, true);
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
        if (className == null || className.trim().equals("")) {
            return null;
        }
        Class clazz = null;
        for (ProxyClassLoader loader : loaders) {
            clazz = loader.loadClass(className, resolveIt);
            if (clazz != null) {
                break;
            }
        }
        if (clazz == null) {
            throw new ClassNotFoundException(className);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name) {
        if (name == null || name.trim().equals("")) {
            return null;
        }
        URL url = null;
        for (ProxyClassLoader loader : loaders) {
            url = loader.findResource(name);
            if (url != null) {
                break;
            }
        }
        return url;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (name == null || name.trim().equals("")) {
            return null;
        }
        InputStream is = null;
        for (ProxyClassLoader loader : loaders) {
            is = loader.loadResource(name);
            if (is != null) {
                break;
            }
        }
        return is;

    }

    private class SystemLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = findSystemClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getSystemResourceAsStream(name);

            if (is != null) {
                return is;
            }

            return null;
        }

        @Override
        public URL findResource(String name) {
            URL url = getSystemResource(name);

            if (url != null) {
                return url;
            }

            return null;
        }
    }

    private class ParentLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = getParent().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getParent().getResourceAsStream(name);

            if (is != null) {
                return is;
            }
            return null;
        }


        @Override
        public URL findResource(String name) {
            URL url = getParent().getResource(name);

            if (url != null) {
                return url;
            }
            return null;
        }
    }

    private static class CurrentLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = getClass().getClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getClass().getClassLoader().getResourceAsStream(name);

            if (is != null) {
                return is;
            }

            return null;
        }


        @Override
        public URL findResource(String name) {
            URL url = getClass().getClassLoader().getResource(name);

            if (url != null) {
                return url;
            }

            return null;
        }
    }

    private class JobUserClassLoader implements ProxyClassLoader {
        private final Map<String, Class> classes = new HashMap<>();
        private final Map<String, ClassLoaderEntry> jarEntries = new HashMap<>();
        private final Map<String, ClassLoaderEntry> dataEntries = new HashMap<>();
        private final Map<String, ClassLoaderEntry> classEntries = new HashMap<>();

        JobUserClassLoader(Map<DeploymentDescriptor, ResourceStream> resourceMap) {
            for (Map.Entry<DeploymentDescriptor, ResourceStream> entry : resourceMap.entrySet()) {
                DeploymentDescriptor descriptor = entry.getKey();
                switch (descriptor.getDeploymentType()) {
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

        @Override
        public Class loadClass(String className, boolean resolveIt) {
            synchronized (getClassLoadingLock(className)) {
                Class result;
                byte[] classBytes;

                result = classes.get(className);

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
                classes.put(className, result);
                return result;
            }
        }


        @Override
        public InputStream loadResource(String name) {
            byte[] arr = classBytes(name);
            if (arr == null) {
                ClassLoaderEntry classLoaderEntry = dataEntries.get(name);
                if (classLoaderEntry != null) {
                    arr = classLoaderEntry.getResourceBytes();
                }
            }
            if (arr == null) {
                return null;
            }
            return new ByteArrayInputStream(arr);
        }

        @Override
        public URL findResource(String name) {
            URL url = getResourceURL(name);
            if (url == null) {
                return null;
            }
            return url;
        }

        private void loadClassStream(DeploymentDescriptor descriptor, ResourceStream resourceStream) {
            byte[] classBytes = JetUtil.readFully(resourceStream.getInputStream());
            classEntries.put(descriptor.getId(), new ClassLoaderEntry(classBytes, resourceStream.getBaseUrl()));
        }

        private void loadDataStream(DeploymentDescriptor descriptor, ResourceStream resourceStream) {
            byte[] bytes = JetUtil.readFully(resourceStream.getInputStream());
            dataEntries.put(descriptor.getId(), new ClassLoaderEntry(bytes, resourceStream.getBaseUrl()));
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

                    int len;
                    byte[] bytes;

                    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                        while ((len = jis.read(b)) > 0) {
                            out.write(b, 0, len);
                        }
                        bytes = out.toByteArray();
                    }

                    String name = jarEntry.getName();
                    String clazzSuffix = ".class";
                    if (jarEntry.getName().endsWith(clazzSuffix)) {
                        name = name.substring(0, name.length() - clazzSuffix.length()).replace("/", ".");
                    }
                    ClassLoaderEntry entry = new ClassLoaderEntry(bytes,
                            "jar:" + resourceStream.getBaseUrl() + "!/" + name);
                    jarEntries.put(name, entry);
                }
            } catch (IOException e) {
                throw new JetClassLoaderException(e);
            } finally {
                JetUtil.close(bis, jis);
            }
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
            ClassLoaderEntry entry = coalesce(name, classEntries, jarEntries);
            if (entry == null) {
                return null;
            }
            return entry.getResourceBytes();

        }

        @SuppressWarnings("unchecked")
        private URL getResourceURL(String name) {
            ClassLoaderEntry entry = coalesce(name, classEntries, dataEntries, jarEntries);
            if (entry == null) {
                return null;
            }
            if (entry.getBaseUrl() == null) {
                throw new JetClassLoaderException("non-URL accessible resource");
            }

            try {
                return new URL(entry.getBaseUrl());
            } catch (MalformedURLException e) {
                throw new JetClassLoaderException(e);
            }

        }
    }
}
