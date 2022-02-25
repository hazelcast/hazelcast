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

import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.MapPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.security.Permission;
import java.util.List;

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
    public static Permission checkRemote(@Nullable String clientXml, @Nonnull Permission permission) {
        if (clientXml == null) {
            return permission;
        }
        return null;
    }


    @Nullable
    public static Permission mapUpdatePermission(@Nullable String clientXml, @Nonnull String name) {
        return checkRemote(clientXml,
                new MapPermission(name, ACTION_CREATE, ACTION_PUT, ACTION_REMOVE, ACTION_READ));
    }

    @Nullable
    public static Permission mapPutPermission(@Nullable String clientXml, @Nonnull String name) {
        return checkRemote(clientXml, new MapPermission(name, ACTION_CREATE, ACTION_PUT));
    }

    @Nullable
    public static Permission cachePutPermission(@Nullable String clientXml, @Nonnull String name) {
        return checkRemote(clientXml, new CachePermission(name, ACTION_CREATE, ACTION_PUT));
    }

    @Nullable
    public static Permission listAddPermission(@Nullable String clientXml, @Nonnull String name) {
        return checkRemote(clientXml, new ListPermission(name, ACTION_CREATE, ACTION_ADD));
    }

    @Nullable
    public static Permission listReadPermission(@Nullable String clientXml, @Nonnull String name) {
        return checkRemote(clientXml, new ListPermission(name, ACTION_CREATE, ACTION_READ));
    }

    public static void checkPermission(SecuredFunction function, ProcessorMetaSupplier.Context context) {
        if (context instanceof Contexts.MetaSupplierCtx) {
            Contexts.MetaSupplierCtx metaSupplierCtx = (Contexts.MetaSupplierCtx) context;
            List<Permission> permissions = function.permissions();
            SecurityContext securityContext = metaSupplierCtx.nodeEngine().getNode().securityContext;
            Subject subject = metaSupplierCtx.subject();
            if (securityContext != null && permissions != null && !permissions.isEmpty() && subject != null) {
                permissions.forEach(permission -> {
                    if (permission != null) {
                        securityContext.checkPermission(subject, permission);
                    }
                });
            }
        }
    }

}
