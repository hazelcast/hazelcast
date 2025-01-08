/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.internal.namespace.ResourceDefinition;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class ResourceDefinitionHolder {
    private final String id;
    private final int resourceType;
    @Nullable
    private final byte[] payload;
    @Nullable
    private final String resourceUrl;

    public ResourceDefinitionHolder(ResourceDefinition resourceDefinition) {
        id = resourceDefinition.id();
        resourceType = resourceDefinition.type().ordinal();
        payload = resourceDefinition.payload();
        if (payload == null) {
            resourceUrl = resourceDefinition.url();
        } else {
            resourceUrl = null;
        }
    }

    public ResourceDefinitionHolder(String id, int resourceType, @Nullable byte[] payload, @Nullable String resourceUrl) {
        this.id = id;
        this.resourceType = resourceType;
        this.payload = payload;
        this.resourceUrl = resourceUrl;
    }

    public String getId() {
        return id;
    }

    public int getResourceType() {
        return resourceType;
    }

    /**
     * This holder can either contain the {@code byte[]} payload, or the URL
     * {@code String} to read the payload from. The direct payload is preferred,
     * so if available it will always be returned by this method.
     *
     * @return the {@code byte[]} for this resource, or {@code null}
     */
    @Nullable
    public byte[] getPayload() {
        return payload;
    }

    /**
     * This holder can either contain the URL {@code String} to read the payload
     * from, or the {@code byte[]} payload itself. The URL is not preferred, so it
     * may be {@code null} if a payload is available.
     * <p>
     * If available, this URL represents one relative to the Hazelcast member.
     *
     * @return the url {@code String} for this resource, or {@code null}
     */
    @Nullable
    public String getResourceUrl() {
        return resourceUrl;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Arrays.hashCode(payload);
        result = (prime * result) + Objects.hash(id, resourceType, resourceUrl);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ResourceDefinitionHolder other = (ResourceDefinitionHolder) obj;
        return Objects.equals(id, other.id) && Arrays.equals(payload, other.payload) && (resourceType == other.resourceType)
                && Objects.equals(resourceUrl, other.resourceUrl);
    }
}
