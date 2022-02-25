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

package com.hazelcast.sql.impl.security;

import java.security.Permission;

/**
 * SQL security context that is used to check for user permissions before the query is executed.
 */
public interface SqlSecurityContext {
    /**
     * Check whether the security is enabled.
     *
     * @return {@code true} if security is enabled
     */
    boolean isSecurityEnabled();

    /**
     * Check whether the user has the given permission.
     *
     * @param permission permission to be checked
     */
    void checkPermission(Permission permission);
}
