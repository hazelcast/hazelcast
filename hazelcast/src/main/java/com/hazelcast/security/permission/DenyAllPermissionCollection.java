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
import java.util.Enumeration;

public class DenyAllPermissionCollection extends PermissionCollection {

    public DenyAllPermissionCollection() {
    }

    @Override
    public void add(Permission permission) {
    }

    @Override
    public boolean implies(Permission permission) {
        return false;
    }

    @Override
    public Enumeration<Permission> elements() {
        return new Enumeration<Permission>() {
            @Override
            public boolean hasMoreElements() {
                return false;
            }

            @Override
            public Permission nextElement() {
                return null;
            }
        };
    }

    @Override
    public int hashCode() {
        return 37;
    }

    @Override
    public String toString() {
        return "<deny all permissions>";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DenyAllPermissionCollection;
    }
}
