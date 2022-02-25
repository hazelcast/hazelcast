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

package com.hazelcast.security;

import com.hazelcast.config.PermissionConfig;

import java.util.Set;

/**
 * Provides necessary methods to initiate security related config changes.
 */
public interface SecurityService {

    /**
     * Propagates changes made to client permissions to all members and reinitiates {@link IPermissionPolicy} with new
     * configuration.
     */
    void refreshClientPermissions(Set<PermissionConfig> permissionConfigs);

    /**
     * Returns latest client permission configuration.
     */
    Set<PermissionConfig> getClientPermissionConfigs();
}
