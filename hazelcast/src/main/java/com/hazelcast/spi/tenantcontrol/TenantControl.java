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

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.tenantcontrol.NoopTenantControl;

import javax.annotation.Nonnull;

/**
 * Hooks for multi-tenancy for application servers. It captures a thread-local
 * context which can then be used to re-establish the same context on different
 * threads.
 * <p>
 * For example, an instance of tenant control can be created on an application
 * thread and this tenant control can then be used to establish the same context
 * on a different thread, e.g.
 * {@link com.hazelcast.spi.impl.operationexecutor.impl.OperationThread}. Operation
 * invocations requiring such context can then be handled correctly.
 * <p>
 * This is used by application servers to establish thread context for class loading,
 * CDI, EJB and JPA invocations
 * <p>
 * <b>Caveats</b>
 * <ul>
 * <li>
 *     Tenant control context is captured on the application thread once the user
 *     interacts with distributed data structure proxies.
 *     In some cases, these proxies might be instantiated as a result of other,
 *     internal, actions such as Hot Restart, WAN replication.
 *     Proxies on the member-side might also be created as a consequence of the
 *     user creating a proxy on a client.
 *     In all these cases, the tenant control context is not captured correctly
 *     and this feature will not work as expected.
 * </li>
 * <li>
 *     When the tenant is not available, operations requiring tenant control
 *     should be re-enqueued for later execution.
 *     This only includes operations which were enqueued on the operation queue
 *     in the first place and does not include operations which were run directly
 *     on the thread running the operations, without being enqueued.
 *     This includes operations that are run as a consequence of another operation,
 *     such as a map.get blocking on a locked entry being unblocked by an unlock
 *     operation.
 * </li>
 * </ul>
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
     * Establish this tenant's thread-local context.
     * The implementation can control the details of what kind of context to set
     * and how to establish it.
     *
     * @return handle to be able to close the tenant's scope.
     */
    Closeable setTenant();

    /**
     * Registers a hook to decouple any Hazelcast object when the tenant is destroyed,
     * This is used, for example, to delete all associated caches from the application
     * when it gets undeployed, so there are no {@link ClassCastException} afterwards.
     *
     * @param destroyEventContext the hook to be used by the tenant control implementation
     *                            Cannot be {@code null}. This is a functional interface, so no-op lambda
     *                            can be used instead.
     */
    void registerObject(@Nonnull DestroyEventContext destroyEventContext);

    /**
     * Invoked when the distributed object belonging to this tenant control has
     * been destroyed. The implementation can undo whatever was done in
     * {@link #registerObject(DestroyEventContext)}. This is so the TenantControl
     * itself can be garbage collected.
     */
    void unregisterObject();

    /**
     * Checks if the tenant app is loaded and classes are available for the given
     * {@link Tenantable} object.
     *
     * @param tenantable passed so the tenant can filter on which object is calling
     * @return {@code true} if tenant is loaded and classes are available
     */
    boolean isAvailable(@Nonnull Tenantable tenantable);

    /**
     * Cleans up all of the thread context to avoid potential class loader leaks
     * This method should clear all potential context items,
     * not just the ones set up in {@link #setTenant()}
     * This acts as a catch-all for any potential class loader and thread-local leaks.
     */
    void clearThreadContext();

    /**
     * Similar to the {@link java.io.Closeable} interface, except the {@link #close()}
     * method does not throw a {@link java.io.IOException}.
     */
    interface Closeable extends AutoCloseable {
        /**
         * Same as Java's close() except no exception is thrown
         */
        @Override
        void close();
    }
}
