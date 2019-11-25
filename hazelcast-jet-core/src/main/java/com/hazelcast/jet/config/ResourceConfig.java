/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.net.URL;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Describes a single resource to deploy to the Jet cluster.
 *
 * @since 3.0
 */
public class ResourceConfig implements Serializable {
    private final URL url;
    private final String id;
    private final ResourceType resourceType;

    /**
     * Creates a resource config with the given properties.
     *
     * @param url           url of the resource
     * @param id            id of the resource
     * @param resourceType  type of the resource
     */
    ResourceConfig(@Nonnull URL url, String id, ResourceType resourceType) {
        checkTrue((resourceType != ResourceType.REGULAR_FILE) ^ id != null,
                "Either archive file or id != null, exclusively");
        this.url = url;
        this.id = id;
        this.resourceType = resourceType;
    }

    /**
     * Creates a config for a class to be deployed. Derives the config
     * properties automatically.
     *
     * @param clazz the class to deploy
     */
    ResourceConfig(Class clazz) {
        id = clazz.getName().replace('.', '/') + ".class";
        url = clazz.getClassLoader().getResource(id);
        checkNotNull(this.url, "Couldn't derive URL from class " + clazz);
        resourceType = ResourceType.REGULAR_FILE;
    }

    /**
     * Returns the URL at which the resource will be available.
     */
    public URL getUrl() {
        return url;
    }

    /**
     * The ID of the resource, null for archive files.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the type of the resource.
     */
    public ResourceType getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return "ResourceConfig{url=" + url + ", id='" + id + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResourceConfig that = (ResourceConfig) o;

        if (url != null ? !url.toString().equals(that.url.toString()) : that.url != null) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return resourceType == that.resourceType;
    }

    @Override
    public int hashCode() {
        int result = url != null ? url.toString().hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (resourceType != null ? resourceType.hashCode() : 0);
        return result;
    }
}
