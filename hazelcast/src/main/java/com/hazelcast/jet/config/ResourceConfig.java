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

package com.hazelcast.jet.config;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;

/**
 * Describes a single resource to deploy to the Jet cluster.
 *
 * @since Jet 3.0
 */
@PrivateApi
public class ResourceConfig implements IdentifiedDataSerializable {

    private URL url;
    private String id;
    private ResourceType resourceType;

    ResourceConfig() {
    }

    /**
     * Creates a resource config with the given properties.
     *
     * @param url           url of the resource
     * @param id            id of the resource
     * @param resourceType  type of the resource
     */
    ResourceConfig(@Nonnull URL url, @Nonnull String id, @Nonnull ResourceType resourceType) {
        Preconditions.checkNotNull(url, "url");
        Preconditions.checkNotNull(resourceType, "resourceType");
        Preconditions.checkHasText(id, "id cannot be null or empty");

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
    ResourceConfig(@Nonnull Class<?> clazz) {
        Preconditions.checkNotNull(clazz, "clazz");

        String id = toClassResourceId(clazz.getName());
        ClassLoader cl = clazz.getClassLoader();
        if (cl == null) {
            throw new IllegalArgumentException(clazz.getName() + ".getClassLoader() returned null, cannot" +
                    " access the class resource. You may have added a JDK class that is loaded by the" +
                    " bootstrap classloader. There is no need to add JDK classes to the job configuration.");
        }
        URL url = cl.getResource(id);
        if (url == null) {
            throw new IllegalArgumentException("The classloader of " + clazz.getName() + " couldn't resolve" +
                    " the resource URL of " + id);
        }

        this.id = id;
        this.url = url;
        this.resourceType = ResourceType.CLASS;
    }

    /**
     * Returns the URL at which the resource is available. Resolved on the
     * local machine during job submission.
     */
    @Nonnull
    public URL getUrl() {
        return url;
    }

    /**
     * Returns the ID of the resource that will be used to form the {@code
     * IMap} key under which it will be stored in the Jet cluster.
     */
    @Nonnull
    public String getId() {
        return id;
    }

    /**
     * Returns the type of the resource.
     */
    @Nonnull
    public ResourceType getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return "ResourceConfig{" +
                "url=" + url +
                ", id='" + id + '\'' +
                ", resourceType=" + resourceType +
                '}';
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
        return url.toString().equals(that.url.toString()) &&
                id.equals(that.id) &&
                resourceType == that.resourceType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, id, resourceType);
    }

    @Override
    public int getFactoryId() {
        return JetConfigDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetConfigDataSerializerHook.RESOURCE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(id);
        out.writeShort(resourceType.getId());
        out.writeObject(url);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readString();
        resourceType = ResourceType.getById(in.readShort());
        url = in.readObject();
    }
}
