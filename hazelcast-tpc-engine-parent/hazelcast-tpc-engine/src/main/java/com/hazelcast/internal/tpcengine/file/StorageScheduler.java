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
 * <p/>
 * The current {@link StorageScheduler} instances are very basic since they
 * only do a FIFO based processing. In the future smarter schedulers could be
 * made; e.g. where there is more control on which storage requests gets
 * preference above others. You could have a background compaction process
 * running with low priority while requests issued by regular users gets
 * precedence.
 */
public interface StorageScheduler {

    /**
     * Allocates a single {@link StorageRequest} instance.
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
     * Schedules a  {@link StorageRequest} so that it is processed at some
     * point in the future. Only StorageRequest that have been allocated using
     * the {@link #allocate()} method on this StorageScheduler should be
     * scheduled.
     *
     * @param req the BlockRequest.
     */
    void schedule(StorageRequest req);

    /**
     * A periodic call that is done to the scheduler to submit staged requests
     * and deal with completed requests if needed.
     */
    boolean tick();
}
