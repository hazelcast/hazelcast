/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.deployment.ResourceDescriptor;
import com.hazelcast.jet.impl.deployment.ResourceKind;

import java.io.Serializable;
import java.net.URL;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Describes a single resource to deploy to the Jet cluster.
 */
public class ResourceConfig implements Serializable {
    private ResourceDescriptor descriptor;
    private URL url;

    /**
     * Creates a resource config with the given properties.
     *
     * @param url  url of the resource
     * @param id   id of the resource
     * @param kind the kind of resource
     */
    public ResourceConfig(URL url, String id, ResourceKind kind) {
        this.descriptor = new ResourceDescriptor(id, kind);
        this.url = url;
    }

    /**
     * Creates a config for a class to be deployed. Derives the config
     * properties automatically.
     *
     * @param clazz the class to deploy
     */
    public ResourceConfig(Class clazz) {
        String classAsPath = clazz.getName().replace('.', '/') + ".class";
        this.url = clazz.getClassLoader().getResource(classAsPath);
        checkNotNull(this.url, "Couldn't derive URL from class " + clazz);
        this.descriptor = new ResourceDescriptor(clazz.getName(), ResourceKind.CLASS);
    }

    /**
     * Returns the URL at which the resource will be available.
     */
    public URL getUrl() {
        return url;
    }

    /**
     * Returns the {@link ResourceDescriptor} for the resource.
     */
    public ResourceDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public String toString() {
        return "{resource=" + descriptor + ", url=" + url + '}';
    }
}
