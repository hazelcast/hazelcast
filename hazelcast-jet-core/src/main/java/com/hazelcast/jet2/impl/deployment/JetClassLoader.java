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

package com.hazelcast.jet2.impl.deployment;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JetClassLoader extends ClassLoader {
    private final List<ClassLoaderDelegate> loaders = new ArrayList<>();

    public JetClassLoader(DeploymentStore deploymentStore) {
        loaders.add(new SystemLoader());
        loaders.add(new ParentLoader());
        loaders.add(new CurrentLoader());
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            loaders.add(new UserClassLoader(deploymentStore));
            return null;
        });

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
        for (ClassLoaderDelegate loader : loaders) {
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
        for (ClassLoaderDelegate loader : loaders) {
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
        for (ClassLoaderDelegate loader : loaders) {
            is = loader.loadResource(name);
            if (is != null) {
                break;
            }
        }
        return is;

    }

    private class SystemLoader implements ClassLoaderDelegate {
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

    private class ParentLoader implements ClassLoaderDelegate {
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

    private static class CurrentLoader implements ClassLoaderDelegate {
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

    private class UserClassLoader implements ClassLoaderDelegate {
        private final Map<String, Class> classes = new HashMap<>();

        private DeploymentStore store;

        UserClassLoader(DeploymentStore deploymentStore) {
            store = deploymentStore;
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
                ClassLoaderEntry classLoaderEntry = store.getDataEntries().get(name);
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
            ClassLoaderEntry entry = coalesce(name, store.getClassEntries(), store.getJarEntries());
            if (entry == null) {
                return null;
            }
            return entry.getResourceBytes();

        }

        @SuppressWarnings("unchecked")
        private URL getResourceURL(String name) {
            ClassLoaderEntry entry = coalesce(name, store.getClassEntries(), store.getDataEntries(), store.getJarEntries());
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

    private interface ClassLoaderDelegate {
        /**
         * Loads the class
         *
         * @param className name of the class
         * @param resolveIt should resolve the class
         * @return Class
         */
        Class loadClass(String className, boolean resolveIt);

        /**
         * Loads the resource
         *
         * @param name resource name
         * @return InputStream
         */
        InputStream loadResource(String name);

        /**
         * Finds the resource
         *
         * @param name resource name
         * @return InputStream
         */
        URL findResource(String name);
    }
}
