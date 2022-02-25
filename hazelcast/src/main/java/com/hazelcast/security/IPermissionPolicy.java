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

import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;

import javax.security.auth.Subject;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Properties;
import java.util.Set;

/**
 * IPermissionPolicy is used to determine any {@link Subject}'s
 * permissions to perform a security sensitive Hazelcast operation.
 */
public interface IPermissionPolicy {

    /**
     * Configures {@link IPermissionPolicy}.
     *
     * @param config Hazelcast {@link Config}
     * @param properties additional properties used to configure the IPermissionPolicy
     */
    void configure(Config config, Properties properties);

    /**
     * Determines permissions of subject.
     *
     * @param subject the {@link Subject}
     * @param type    of permissions in PermissionCollection
     * @return PermissionCollection containing subject's permissions
     */
    PermissionCollection getPermissions(Subject subject, Class<? extends Permission> type);

    /**
     * Replaces existing permissions with given set of permissions. Implementation must take into account that
     * {@code refreshPermissions} may be executed concurrently with {@code getPermissions}.
     *
     * @param permissionConfigs the new set of permissions to apply
     */
    void refreshPermissions(Set<PermissionConfig> permissionConfigs);

    /**
     * Destroys {@link IPermissionPolicy}.
     */
    void destroy();
}
