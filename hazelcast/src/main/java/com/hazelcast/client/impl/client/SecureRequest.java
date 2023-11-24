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

package com.hazelcast.client.impl.client;

import javax.annotation.Nullable;
import java.security.Permission;

public interface SecureRequest {

    Permission getRequiredPermission();

    /**
     * Defines the {@link com.hazelcast.security.permission.NamespacePermission} associated
     * with this request, if applicable. Since the majority of requests are not associated
     * with a Namespace, this method returns {@code null} by default to reduce bloat.
     * <p>
     * Requests that are associated with a {@code Namespace} should implement this method
     * and return the appropriate {@link com.hazelcast.security.permission.NamespacePermission}
     *
     * @return The {@link com.hazelcast.security.permission.NamespacePermission} required for
     *         this task, or {@code null} if there is no Namespace associated with it.
     */
    @Nullable
    default Permission getNamespacePermission() {
        return null;
    }

    /**
     * Used for {@link com.hazelcast.security.SecurityInterceptor}
     * @return
     */
    String getDistributedObjectType();

    /**
     * Used for {@link com.hazelcast.security.SecurityInterceptor}
     * @return
     */
    String getDistributedObjectName();

    /**
     * Used for {@link com.hazelcast.security.SecurityInterceptor}
     * Method name which called via a distributedObject
     * for map.put, methodName will be 'put'
     * For requests which do not produced via a distributedObject should return null, for example internal client.
     * @return
     */
    String getMethodName();

    /**
     * Used for {@link com.hazelcast.security.SecurityInterceptor}
     * Parameters passed to the method by a distributedObject
     * for map.put(key, value) parameters should be 'key' and 'value'
     * parameters can be in binary or object form, underlying implementation will de-serialize lazily
     * @return
     */
    Object[] getParameters();
}
