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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ClusterPermissionCollection extends PermissionCollection {

    final Set<Permission> perms = new HashSet<Permission>();
    final Class<? extends Permission> permClass;

    public ClusterPermissionCollection() {
        permClass = null;
    }

    public ClusterPermissionCollection(Class<? extends Permission> permClass) {
        this.permClass = permClass;
    }

    public void add(Permission permission) {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        boolean shouldAdd = (permClass != null && permClass.equals(permission.getClass()))
                || (permission instanceof ClusterPermission);

        if (shouldAdd && !implies(permission)) {
            perms.add(permission);
        }
    }

    public void add(PermissionCollection permissions) {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        if (permissions instanceof ClusterPermissionCollection) {
            for (Permission p : ((ClusterPermissionCollection) permissions).perms) {
                add(p);
            }
        }
    }

    public boolean implies(Permission permission) {
        for (Permission p : perms) {
            if (p.implies(permission)) {
                return true;
            }
        }
        return false;
    }

    public void compact() {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        final Iterator<Permission> iter = perms.iterator();
        while (iter.hasNext()) {
            final Permission perm = iter.next();
            boolean implies = false;
            for (Permission p : perms) {
                if (p != perm && p.implies(perm)) {
                    implies = true;
                    break;
                }
            }
            if (implies) {
                iter.remove();
            }
        }
        setReadOnly();
    }

    public Enumeration<Permission> elements() {
        return Collections.enumeration(perms);
    }

    public Set<Permission> getPermissions() {
        return Collections.unmodifiableSet(perms);
    }

    @Override
    public String toString() {
        return "ClusterPermissionCollection [permClass=" + permClass + "]";
    }
}
