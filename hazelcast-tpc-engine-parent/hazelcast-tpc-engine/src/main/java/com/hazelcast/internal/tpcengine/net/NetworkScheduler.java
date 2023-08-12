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

package com.hazelcast.internal.tpcengine.net;

/**
 * A scheduler specific networking requests from the {@link AsyncSocket}.
 * <p>
 * Currently only writes to the network go through the Scheduler.
 * <p>
 * Todo:
 * Currently only dirty sockets are registered. And it isn't comparable with the
 * storage scheduler where storage-requests are scheduled.
 *
 * @param <S>
 */
public interface NetworkScheduler<S extends AsyncSocket> {

    /**
     * Schedules a dirty socket to be written to the network at some point in
     * the future.
     *
     * @param socket the AsyncSocket to schedule.
     */
    void schedule(S socket);

    default void unsafeSchedule(S socket) {
        schedule(socket);
    }

    boolean hasPending();

    void tick();
}
