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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.tenantcontrol.NoopTenantControlFactory;

/**
 * A {@code TenantControlFactory} supplies {@link TenantControl} objects when
 * Hazelcast service attaches the tenant control to a distributed object.
 *
 * An implementation of {@code TenantControlFactory} is instantiated
 * via {@link com.hazelcast.internal.util.ServiceLoader}, so in order to be picked up:
 * <ul>
 *     <li>Its class name must be stored in a service definition file in
 *     {@code META-INF/services/com.hazelcast.spi.tenantcontrol.TenantControlFactory}</li>
 *     <li>It must have a public no-args constructor</li>
 * </ul>
 */
@Beta
public interface TenantControlFactory {
    /**
     * Default tenant control factory. Always produces {@link TenantControl#NOOP_TENANT_CONTROL}
     */
    TenantControlFactory NOOP_TENANT_CONTROL_FACTORY = new NoopTenantControlFactory();

    /**
     * To be called from the application's thread to connect a Hazelcast object
     * with a particular tenant, e.g. JCache-based cache with a particular application
     * Implementor will save the current thread context and return it
     * Further operations from other threads will use the returned context
     * for this particular Hazelcast object to re-establish the invocation context
     *
     * @return new TenantControl instance with the saved state of the current tenant
     */
    TenantControl saveCurrentTenant();

    /**
     * If the method returns false (classes are not always available),
     * all objects, e.g. CacheConfigs are always sent over the wire
     * in a form that does not require classes to be loaded / available.
     * Objects will be sent using class names instead of class types,
     * and sending byte arrays instead of serialized objects.
     * This is so de-serialization does not fail with ClassNotFoundException
     *
     * @return true is the associated applications are always loaded and running
     */
    boolean isClassesAlwaysAvailable();
}
