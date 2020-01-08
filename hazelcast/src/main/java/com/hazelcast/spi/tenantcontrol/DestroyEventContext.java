/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.spi.annotation.Beta;

/**
 * Hook to decouple Hazelcast object from the tenant
 * @param <T> context type
 */
@Beta
public interface DestroyEventContext<T> {

    /**
     * Called to decouple Hazelcast object from the tenant
     *
     * @param context to use to destroy the Hazelcast object
     */
    void destroy(T context);

    /**
     * @return context type so the tenant control implementor knows
     * what context to send to the destroy() method
     */
    Class<? extends T> getContextType();

    /**
     *
     * @return the name of the distributed object
     */
    String getDistributedObjectName();

    /**
     *
     * @return the service name
     */
    String getServiceName();
}
