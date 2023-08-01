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
 * A BlockRequestScheduler is bound to a single Reactor and can only process
 * {@link StorageRequest} instances from {@link AsyncFile} that belong to that
 * reactor.
 */
public interface StorageScheduler {

    /**
     * Reserves a single IO. The IO is not submitted to io_uring yet.
     * <p/>
     * If a non null value is returned, it is guaranteed that
     * {@link #submit(StorageRequest)} will complete successfully.
     *
     * @return the reserved IO or null if there is no space.
     */
    StorageRequest allocate();

    /**
     * Submits a BlockRequest so that it is processed at some point in the
     * future. Only BlockRequest that have been allocated using the
     * {@link #allocate()} method on this BlockRequestScheduler should be
     * submitted.
     *
     * @param req the BlockRequest.
     */
    void submit(StorageRequest req);
}
