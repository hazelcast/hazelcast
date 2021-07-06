/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.MapPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;

import static com.hazelcast.security.permission.ActionConstants.ACTION_ADD;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.security.permission.ActionConstants.ACTION_REMOVE;

/**
 * Utility class for security permissions.
 */
public final class PermissionsUtil {

    private PermissionsUtil() {
    }

    @Nullable
    public static Permission checkRemote(@Nullable ClientConfig clientConfig, @Nonnull Permission permission) {
        if (clientConfig == null) {
            return permission;
        }
        return null;
    }


    @Nullable
    public static Permission mapUpdatePermission(@Nullable ClientConfig clientConfig, @Nonnull String name) {
        return checkRemote(clientConfig,
                new MapPermission(name, ACTION_CREATE, ACTION_PUT, ACTION_REMOVE, ACTION_READ));
    }

    @Nullable
    public static Permission mapPutPermission(@Nullable ClientConfig clientConfig, @Nonnull String name) {
        return checkRemote(clientConfig, new MapPermission(name, ACTION_CREATE, ACTION_PUT));
    }

    @Nullable
    public static Permission cachePutPermission(@Nullable ClientConfig clientConfig, @Nonnull String name) {
        return checkRemote(clientConfig, new CachePermission(name, ACTION_CREATE, ACTION_PUT));
    }

    @Nullable
    public static Permission listAddPermission(@Nullable ClientConfig clientConfig, @Nonnull String name) {
        return checkRemote(clientConfig, new ListPermission(name, ACTION_CREATE, ACTION_ADD));
    }

    @Nullable
    public static Permission listReadPermission(@Nullable ClientConfig clientConfig, @Nonnull String name) {
        return checkRemote(clientConfig, new ListPermission(name, ACTION_CREATE, ACTION_READ));
    }
}
