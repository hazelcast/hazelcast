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
 * Currently only dirty sockets are registered. And it isn't comparable with the
 * storage scheduler where storage-requests are scheduled.
 *
 * @param <S>
 */
public interface NetworkScheduler<S extends AsyncSocket> {

    /**
     * Schedules a dirty socket to be written to the network at some point in
     * the future.
     * <p/>
     * A dirty socket should only schedule itself once when it is dirty. If
     * a socket schedules itself more than once, there is a bug.
     *
     * This call should be made from the eventloop thread and isn't threadsafe.
     *
     * @param socket the AsyncSocket to schedule.
     * @throws IllegalStateException if the scheduler exceeds the limit of
     *                               sockets it can schedule. This should not
     *                               happen because the NetworkScheduler should
     *                               be sized based on the socketLimit.
     */
    void scheduleWrite(S socket);

    /**
     * Checks if there are any dirty sockets pending.
     *
     * @return true if there are any dirty sockets pending.
     */
    boolean hasPending();

    /**
     * Gives the NetworkScheduler to do some work by giving it a tick.
     *
     * @return true if any work was done, false otherwise.
     */
    boolean tick();
}
