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

package com.hazelcast.security.permission;

import java.security.Permission;

/**
 * Hazelcast Permission type used in client protocol actions intended for Management Center operations. It has a similar
 * behavior as the {@link RuntimePermission} - i.e. actions are not used and the permission name can end with a wildcard
 * <code>".*"</code> (e.g. <code>"cluster.*"</code>).
 * <p>
 * The {@code "*"} (star character) or {@code null} used as the name means that such permission {@code implies} all other
 * {@link ManagementPermission} instances.
 */
public class ManagementPermission extends ClusterPermission {

    final String prefix;

    public ManagementPermission(String name) {
        super(name);
        prefix = (name != null && name.endsWith(".*")) ? name.substring(0, name.length() - 1) : null;
    }

    @Override
    public boolean implies(Permission permission) {
        if (permission == null || getClass() != permission.getClass()) {
            return false;
        }
        ManagementPermission that = (ManagementPermission) permission;
        String name = getName();
        String thatName = that.getName();
        if (name == null || name.equals("*") || name.equals(thatName) || (prefix != null && thatName.startsWith(prefix))) {
            return true;
        }
        return false;
    }

    @Override
    public String getActions() {
        return null;
    }

    @Override
    public String toString() {
        return "ManagementPermission [getName()=" + getName() + "]";
    }
}
