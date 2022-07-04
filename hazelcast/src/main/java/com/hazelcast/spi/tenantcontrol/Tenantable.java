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

/**
 * Interface to be implemented by classes which can be queried for requirement
 * of a tenant context.
 */
@Beta
public interface Tenantable {

    /**
     * @return {@code true} when this object requires a tenant context to be set,
     * for example in order to resolve user classes, otherwise {@code false}.
     */
    boolean requiresTenantContext();

    /**
     * Retrieves the tenant control relevant for this given object. Tenant control
     * should already have been set up on this Hazelcast instance somehow, for
     * instance by calling {@link TenantControlFactory#saveCurrentTenant()} on a
     * user thread or should have been replicated from another Hazelcast instance.
     *
     * @return tenant control responsible for this object
     */
    TenantControl getTenantControl();
}
