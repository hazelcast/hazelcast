/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nonblocking;

/**
 * A nio event handler that supports migration between {@link NonBlockingIOThread} instances.
 * This API is called by the {@link com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer}.
 */
public interface MigratableHandler {

    /**
     * Requests the MigratableHandler to move to the new NonBlockingIOThread. This call will not wait for the
     * migration to complete.
     *
     * This method can be called by any thread, and will probably be called by the
     * {@link com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer}.
     *
     * Call is ignored when handler is moving to the same NonBlockingIOThread.
     *
     * @param newOwner the NonBlockingIOThread that is going to own this MigratableHandler
     */
    void requestMigration(NonBlockingIOThread newOwner);

    /**
     * Get NonBlockingIOThread currently owning this handler. Handler owner is a thread running this handler.
     * {@link com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer IOBalancer} can decide to migrate
     * a handler to another owner.
     *
     * @return current owner
     */
    NonBlockingIOThread getOwner();

    /**
     * Get number of events recorded by the current handler. It can be used to calculate whether
     * this handler should be migrated to a different {@link NonBlockingIOThread}
     *
     * @return total number of events recorded by this handler
     */
    long getEventCount();
}
