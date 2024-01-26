/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

public class ResourceDefinitionImpl implements ResourceDefinition {
    private String id;
    private byte[] payload;
    private ResourceType type;
    private String url;

    public ResourceDefinitionImpl() {
    }

    public ResourceDefinitionImpl(String id, byte[] payload, ResourceType type, String url) {
        this.id = id;
        this.payload = payload;
        this.type = type;
        this.url = url;
    }

    public ResourceDefinitionImpl(ResourceConfig resourceConfig) {
        id = resourceConfig.getId();
        payload = readPayloadFromUrl(id, resourceConfig.getUrl());
        type = resourceConfig.getResourceType();
        url = resourceConfig.getUrl().toString();
    }

    public ResourceDefinitionImpl(ResourceDefinitionHolder holder) {
        id = holder.getId();
        payload = holder.getPayload();
        type = ResourceType.getById(holder.getResourceType());
        url = holder.getResourceUrl();

        // ResourceDefinitionHolder can have a null payload, and we should read from URL like above
        if (payload == null) {
            if (url == null) {
                throw new IllegalStateException("URL is not defined while payload is null for resource id " + id);
            }
            try {
                payload = readPayloadFromUrl(id, new URI(url).toURL());
            } catch (URISyntaxException | MalformedURLException ex) {
                throw new IllegalArgumentException("Encountered invalid URL for resource id " + id + " and URL " + url, ex);
            }
        }
    }

    private byte[] readPayloadFromUrl(String id, URL url) {
        try (InputStream is = url.openStream()) {
            return is.readAllBytes();
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not open stream for resource id " + id + " and URL " + url, e);
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public ResourceType type() {
        return type;
    }

    @Override
    public byte[] payload() {
        return payload;
    }

    @Override
    @Nullable
    public String url() {
        return url;
    }

    @Override
    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.RESOURCE_DEFINITION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(id);
        out.writeByteArray(payload);
        out.writeInt(type.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readString();
        payload = in.readByteArray();
        type = ResourceType.getById(in.readInt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceDefinitionImpl that = (ResourceDefinitionImpl) o;
        return Objects.equals(id, that.id)
                && Arrays.equals(payload, that.payload)
                && type == that.type
                && Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, type, url);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
