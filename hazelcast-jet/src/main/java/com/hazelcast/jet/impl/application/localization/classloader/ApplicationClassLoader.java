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

import java.net.URL;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import com.hazelcast.jet.api.application.localization.LocalizationStorage;
import com.hazelcast.jet.impl.util.JetUtil;

public class ApplicationClassLoader extends ClassLoader {
    protected final List<ProxyClassLoader> loaders = new ArrayList<ProxyClassLoader>();

    private final ProxyClassLoader systemLoader = new SystemLoader();
    private final ProxyClassLoader parentLoader = new ParentLoader();
    private final ProxyClassLoader currentLoader = new CurrentLoader();

    public ApplicationClassLoader(LocalizationStorage localizationStorage) {
        addDefaultLoaders();

        try {
            addLoader(new LocalizationClassLoader(localizationStorage.getResources()));
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private void addDefaultLoaders() {
        addLoader(this.systemLoader);
        addLoader(this.parentLoader);
        addLoader(this.currentLoader);
    }

    public void addLoader(ProxyClassLoader loader) {
        this.loaders.add(loader);
    }

    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        return (loadClass(className, true));
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
        if (className == null || className.trim().equals("")) {
            return null;
        }

        Class clazz = null;

        for (ProxyClassLoader l : this.loaders) {
            clazz = l.loadClass(className, resolveIt);

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

        for (ProxyClassLoader l : this.loaders) {
            url = l.findResource(name);
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

        for (ProxyClassLoader l : this.loaders) {
            is = l.loadResource(name);

            if (is != null) {
                break;
            }
        }

        return is;

    }

    /**
     * System class loader
     */
    class SystemLoader implements ProxyClassLoader {
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

    /**
     * Parent class loader
     */
    class ParentLoader implements ProxyClassLoader {
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

    /**
     * Current class loader
     */
    class CurrentLoader implements ProxyClassLoader {
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
}
