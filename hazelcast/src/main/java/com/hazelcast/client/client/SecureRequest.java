/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.client;

import java.security.Permission;

public interface SecureRequest {

    Permission getRequiredPermission();

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
