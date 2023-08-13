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

package com.hazelcast.internal.tpcengine.file;

/**
 * The scheduler for {@link StorageRequest}s.
 * <p/>
 * A StorageScheduler is bound to a single Reactor and can only process
 * {@link StorageRequest} instances from {@link AsyncFile} that belong to that
 * reactor.
 */
public interface StorageScheduler {

    /**
     * Allocates a single StorageRequest instance.
     * <p/>
     * If a non <code>null</code> value is returned, it is guaranteed that
     * {@link #schedule(StorageRequest)} will complete successfully;
     * although it doesn't say anything if the actual request will complete
     * successfully.
     *
     * @return the allocated StorageRequest or null if there is no space.
     */
    StorageRequest allocate();

    /**
     * Schedules a StorageRequest so that it is processed at some point in the
     * future. Only StorageRequest that have been allocated using the
     * {@link #allocate()} method on this StorageScheduler should be
     * submitted.
     *
     * @param req the BlockRequest.
     */
    void schedule(StorageRequest req);

    /**
     * A period call that is done to the scheduler to submit staged requests
     * and deal with completed requests if needed.
     */
    void tick();
}
