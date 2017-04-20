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

package com.hazelcast.jet.impl.deployment;

import java.io.Serializable;

public class ResourceDescriptor implements Serializable {
    private final String id;
    private final ResourceKind resourceKind;

    public ResourceDescriptor(String id, ResourceKind resourceKind) {
        this.id = id;
        this.resourceKind = resourceKind;
    }

    public String getId() {
        return id;
    }

    ResourceKind getResourceKind() {
        return resourceKind;
    }

    @Override
    public boolean equals(Object o) {
        ResourceDescriptor that;
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.id.equals((that = (ResourceDescriptor) o).id)
                && this.resourceKind == that.resourceKind;
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 37 * hc + id.hashCode();
        hc = 37 * hc + resourceKind.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return id + " (" + getResourceKind() + ')';
    }
}
