/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.permission;

import java.security.Permission;
import java.security.PermissionCollection;

public abstract class ClusterPermission extends Permission {

    private int hashcode;

    public ClusterPermission(String name) {
        super(name);
    }

    public PermissionCollection newPermissionCollection() {
        return new ClusterPermissionCollection(getClass());
    }

    @Override
    public int hashCode() {
        if (hashcode == 0) {
            final int prime = 31;
            int result = 1;
            if (getName() == null) {
                result = prime * result + 13;
            } else {
                result = prime * result + getName().hashCode();
            }
            hashcode = result;
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ClusterPermission other = (ClusterPermission) obj;
        if (getName() == null && other.getName() != null) {
            return false;
        }
        if (!getName().equals(other.getName())) {
            return false;
        }
        return true;
    }
}
