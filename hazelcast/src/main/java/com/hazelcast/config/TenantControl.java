/*
 * Copyright 2017-2018 Hazelcast, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.config;

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
public interface TenantControl extends Serializable {
    /**
     * To be called from the application's thread to connect a Hazelcast object
     * with a particular tenant, e.g. JCache-based cache with a particular application
     * Implementor will save the current thread context and return it
     * Further operations from other threads will use the returned context
     * for this particular Hazelcast object to re-establish the invocation context
     *
     * @param event hook to destroy any Hazelcast object when the tenant is destroyed,
     * This is used, for example, to delete all associated caches from the application when
     * it gets undeployed, so there are no ClassCastExceptions afterwards
     *
     * @return new TenantControl instance with the saved state of the current tenant
     */
    TenantControl saveCurrentTenant(DestroyEventContext event);

    /**
     * Establish this tenant's thread-local context
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
    public void unregister();

    /**
     * Hook to decouple Hazelcast object from the tenant
     */
    interface DestroyEventContext extends Serializable {
        /**
         * called to decouple Hazelcast object from the tenant
         *
         * @param <TT> context type
         * @param context to use to delete the cache
         */
        <TT> void destroy(TT context);

        /**
         * @return context type so the tenant control implementor knows
         * what context to send to the destroy() method
         */
        Class<?> getContextType();
    }

    /**
     * Cannot use AutoCloseable due to JDK 6 compatibility
     * Used to remove IOException checked exception, because there is no need
     */
    interface Closeable extends java.io.Closeable {
        /**
         * Remove IOException so it doesn't have to be caught
         */
        @Override
        public void close();
    };

    /**
     * Default no-op implementation of TenantControl
     * This is so the code doesn't have to do any null checks
     */
    class NoTenantControl implements TenantControl {
        @Override
        public TenantControl saveCurrentTenant(DestroyEventContext event) {
            return this;
        }

        @Override
        public void unregister() {
        }

        @Override
        public TenantControl.Closeable setTenant(boolean createRequestScope) {
            return new Closeable() {
                @Override
                public void close() {
                }
            };
        }

        private static final long serialVersionUID = 1L;
    }
}
