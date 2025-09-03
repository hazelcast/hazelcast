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

package com.hazelcast.config.security;

import javax.naming.directory.SearchControls;

/**
 * Search scope types for LDAP queries.
 */
public enum LdapSearchScope {
    /**
     * Only given search context object.
     */
    OBJECT("object", SearchControls.OBJECT_SCOPE),
    /**
     * One level under the search context.
     */
    ONE_LEVEL("one-level", SearchControls.ONELEVEL_SCOPE),
    /**
     * Subtree of the search context.
     */
    SUBTREE("subtree", SearchControls.SUBTREE_SCOPE);

    /**
     * Default search scope for LDAP searches.
     */
    private static final LdapSearchScope DEFAULT = LdapSearchScope.SUBTREE;

    private final String valueString;
    private final int searchControlValue;

    LdapSearchScope(String valueString, int searchControlValue) {
        this.valueString = valueString;
        this.searchControlValue = searchControlValue;
    }

    @Override
    public String toString() {
        return valueString;
    }

    public int toSearchControlValue() {
        return searchControlValue;
    }

    public static LdapSearchScope getSearchScope(String label) {
        if (label != null) {
            label = label.strip();
            for (LdapSearchScope scope : LdapSearchScope.values()) {
                if (scope.toString()
                        .equals(label)) {
                    return scope;
                }
            }
        }
        return DEFAULT;
    }
}
