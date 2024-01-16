/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.access.impl;

import javax.security.auth.login.LoginException;

import com.hazelcast.access.AccessControlService;
import com.hazelcast.access.AuthenticationContext;
import com.hazelcast.access.AuthorizationContext;

public class NoOpAccessControlService implements AccessControlService {

    public static final NoOpAccessControlService INSTANCE = new NoOpAccessControlService();
    private static final String[] ROLES = new String[0];

    private NoOpAccessControlService() {
    }

    @Override
    public String[] authenticate(AuthenticationContext ctx) throws LoginException {
        return ROLES;
    }

    @Override
    public boolean isAccessGranted(AuthorizationContext ctx, String... assignedRoles) {
        return true;
    }

}
