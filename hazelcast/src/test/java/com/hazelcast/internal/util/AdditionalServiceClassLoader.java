/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Vector;

/**
 * Uses parent classloader to look up service provider files from {@code additional-services/...}
 * when asked for {@code META-INF/services/} resources. This makes for easier testing of ServiceLoader
 * related functionality.
 */
public class AdditionalServiceClassLoader
        extends URLClassLoader {

    private static final String SERVICE_PROVIDER_PREFIX = "META-INF/services";
    private static final String ADDITIONAL_SERVICES_PREFIX = "additional-services";

    public AdditionalServiceClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException {
        Enumeration<URL> resources = getParent().getResources(name);
        if (name.startsWith(SERVICE_PROVIDER_PREFIX)) {
            String additionalServiceName = name.replace(SERVICE_PROVIDER_PREFIX, ADDITIONAL_SERVICES_PREFIX);
            URL additionalResource = getParent().getResource(additionalServiceName);
            if (additionalResource != null) {
                resources = addResource(resources, additionalResource);
            }
        }
        return resources;
    }

    private Enumeration<URL> addResource(Enumeration<URL> original, URL additionalResource) {
        Vector<URL> urls = new Vector<URL>();
        while (original.hasMoreElements()) {
            urls.add(original.nextElement());
        }
        urls.add(additionalResource);
        return urls.elements();
    }
}
