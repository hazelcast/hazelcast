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

public final class AllPermissions extends ClusterPermission {

    public AllPermissions() {
        super("<all permissions>");
    }

    public boolean implies(Permission permission) {
        return true;
    }

    public String getActions() {
        return "<all actions>";
    }

    public String toString() {
        return "<allow all permissions>";
    }

    public boolean equals(Object obj) {
        return obj instanceof AllPermissions;
    }

    public int hashCode() {
        return 111;
    }

    public PermissionCollection newPermissionCollection() {
        return new AllPermissionsCollection();
    }

    public static final class AllPermissionsCollection extends PermissionCollection {
        private static final AllPermissions ALL_PERMISSIONS = new AllPermissions();
        private boolean all = false;

        public AllPermissionsCollection() {
            super();
        }

        public AllPermissionsCollection(boolean all) {
            super();
            this.all = all;
        }

        public void add(Permission permission) {
            if (permission instanceof AllPermissions) {
                all = true;
            }
        }

        public boolean implies(Permission permission) {
            return all;
        }

        public Enumeration<Permission> elements() {
            return new Enumeration<Permission>() {
                boolean more = all;

                public boolean hasMoreElements() {
                    return more;
                }

                public Permission nextElement() {
                    more = false;
                    return ALL_PERMISSIONS;
                }
            };
        }

        public int hashCode() {
            return all ? 13 : -13;
        }

        public String toString() {
            return "<allow all permissions>";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllPermissionsCollection other = (AllPermissionsCollection) obj;
            return all == other.all;
        }
    }

}
