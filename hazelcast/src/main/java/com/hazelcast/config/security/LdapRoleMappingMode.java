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

package com.hazelcast.config.security;

import static com.hazelcast.internal.util.StringUtil.trim;

/**
 * Enum for LDAP role mapping modes.
 */
public enum LdapRoleMappingMode {
    /**
     * Role name in user object attribute.
     */
    ATTRIBUTE("attribute"),
    /**
     * User object attribute contains DNs of role objects.
     */
    DIRECT("direct"),
    /**
     * Role object attribute contains DN of user objects.
     */
    REVERSE("reverse");

    /**
     * Default role mapping mode in LDAP authentication configuration.
     */
    private static final LdapRoleMappingMode DEFAULT = LdapRoleMappingMode.ATTRIBUTE;

    private final String valueString;

    LdapRoleMappingMode(String valueString) {
        this.valueString = valueString;
    }

    @Override
    public String toString() {
        return valueString;
    }

    public static LdapRoleMappingMode getRoleMappingMode(String label) {
        label = trim(label);
        if (label == null) {
            return DEFAULT;
        }
        for (LdapRoleMappingMode mode : LdapRoleMappingMode.values()) {
            if (mode.toString().equals(label)) {
                return mode;
            }
        }
        return DEFAULT;
    }
}
