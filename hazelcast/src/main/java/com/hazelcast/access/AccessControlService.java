/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.access;

import javax.annotation.Nonnull;
import javax.security.auth.login.LoginException;

/**
 * Service for pluggable authentication and authorization.
 */
public interface AccessControlService {

    /**
     * Authenticates user described by given {@link AuthenticationContext} and returns role names assigned.
     *
     * @param ctx authentication context
     * @return array of role names assigned to authenticated user
     * @throws LoginException authentication fails
     */
    @Nonnull
    String[] authenticate(@Nonnull AuthenticationContext ctx) throws LoginException;

    /**
     * Returns {@code true} when access to resource described in the {@link AuthorizationContext} should be granted to assigned
     * roles. The role names are typically the ones returned by the {@link #authenticate(AuthenticationContext)} method call.
     *
     * @param ctx authorization context
     * @param assignedRoles role names to be checked for access to the resource
     * @return {@code true} when the access is granted
     */
    boolean isAccessGranted(@Nonnull AuthorizationContext ctx, @Nonnull String... assignedRoles);
}
