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

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.tenantcontrol.NoopTenantControl;
import javax.annotation.Nonnull;

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
public interface TenantControl extends DataSerializable {
    /**
     * Default no-op tenant control
     */
    TenantControl NOOP_TENANT_CONTROL = new NoopTenantControl();

    /**
     * Establish this tenant's thread-local context
     * Particular TenantControl implementation will control the details of
     * what kind of context to set and how to establish it
     *
     * @return handle to be able to close the tenant's scope.
     */
    Closeable setTenant();

    /**
     * Called when Hazelcast object is created
     * @param destroyEventContext hook to decouple any Hazelcast object when the tenant is destroyed,
     * This is used, for example, to delete all associated caches from the application when
     * it gets undeployed, so there are no {@link ClassCastException} afterwards.
     * Cannot be {@code null}. This is a functional interface, so no-op lambda can be used instead.
     */
    void registerObject(@Nonnull DestroyEventContext destroyEventContext);

    /**
     * Called when the Hazelcast object that's attached to this tenant is destroyed.
     * Implementing class can undo whatever was done in {@link #registerObject(DestroyEventContext)}
     * This is so the TenantControl itself can be garbage collected
     */
    void unregisterObject();

    /**
     * Checks if tenant app is loaded at the current time and classes are available
     * for the given {@link Tenantable} object.
     *
     * @param tenantable    passed so the tenant can filter on which object is calling
     * @return              true if tenant is loaded and classes are available
     */
    boolean isAvailable(Tenantable tenantable);

    /**
     * Clean up all the thread context to avoid potential class loader leaks
     * This method should clear all potential context items,
     * not just the ones set up in {@link #setTenant()}
     * This acts as a catch-all for any potential class class loader and thread-local leaks
     */
    void clearThreadContext();

    /**
     * same to Java's Closeable interface, except close() method does not throw IOException
     */
    interface Closeable extends AutoCloseable {
        /**
         * Same as Java's close() except no exception is thrown
         */
        @Override
        void close();
    }
}
