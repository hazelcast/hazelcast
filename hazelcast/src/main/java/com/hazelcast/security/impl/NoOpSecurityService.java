/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.security.SecurityService;

import java.util.Collections;
import java.util.Set;

/**
 * Empty implementation of {@link SecurityService}, used when security is not enabled.
 */
public class NoOpSecurityService implements SecurityService {

    private final Set<PermissionConfig> permissionConfigs;

    public NoOpSecurityService(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {

    }

    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        return Collections.unmodifiableSet(permissionConfigs);
    }
}
