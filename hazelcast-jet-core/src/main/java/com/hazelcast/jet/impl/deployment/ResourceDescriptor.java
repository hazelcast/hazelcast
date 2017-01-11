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

    public ResourceKind getResourceKind() {
        return resourceKind;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResourceDescriptor that = (ResourceDescriptor) o;

        if (!this.id.equals(that.id)) {
            return false;
        }

        return resourceKind == that.resourceKind;

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + resourceKind.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return id + " (" + getResourceKind() + ")";
    }
}
