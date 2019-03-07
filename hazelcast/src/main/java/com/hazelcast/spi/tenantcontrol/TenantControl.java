/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.tenantcontrol.NoopTenantControl;

import java.io.Closeable;
import java.io.Serializable;

/**
 * Hooks for multi-tenancy for application servers
 * Hazelcast uses separate threads to invoke operations
 * this interface acts a hook to establish a thread-local tenant context
 * so that operation invocations into application servers are handled correctly
 * This is used by application servers to establish thread context for class loading,
 * CDI, EJB and JPA invocations
 *
 * @author lprimak
 */
@Beta
public interface TenantControl extends Serializable {

    /**
     * Default no-op tenant control
     */
    TenantControl NOOP_TENANT_CONTROL = new NoopTenantControl();

    /**
     * Establish this tenant's thread-local context
     * Particular TenantControl implementation will control the details of how
     * createRequestScope parameter is handled, but in general,
     * if createRequestScope = false, only ClassLoader is set up,
     * if createRequestScope = true, in addition to ClassLoader,
     * other things like invocation, EJB/JPA/CDI context should be set up as well
     *
     * In other words, if only app class needs to be resolved, set createRequestScope to false
     * If actually calling into user's code, set createRequestScope to true
     *
     * @param createRequestScope whether to create CDI request scope for this context
     * @return handle to be able to close the tenant's scope.
     */
    Closeable setTenant(boolean createRequestScope);

    /**
     * To be called when the Hazelcast object attached to this tenant is destroyed.
     * The implementor may unregister it's own event listeners here.
     * This is used with conjunction with DestroyEvent, because
     * the listeners are probably used to call the DestroyEvent,
     * this just acts as the other event that will decouple
     * Hazelcast object from the tenant
     * This is so the TenantControl itself can be garbage collected
     */
    void unregister();
}
