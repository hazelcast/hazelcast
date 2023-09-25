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

package com.hazelcast.internal.tpcengine.util;

/**
 * Returns the time in nanoseconds from when this clock was created.
 * <p/>
 * The difference between {@link NanoClock} and {@link System#nanoTime()} is that
 * with NanoClock, the start time is very recent, so there is a huge period between now
 * and overflowing. With the System.nanoTime, it could be close to overflowing all the time
 * and this is problematic when dealing with deadlines because they could become negative
 * and you can't use for example a priority queue for scheduled tasks.
 * <p/>
 * NanoClocks are not required to be threadsafe.
 * <p/>
 * The time from different NanoClock can't be compared with each other.
 */
public interface NanoClock {

    /**
     * Returns the time in nanoseconds from when this NanoClock was created.
     *
     * @return the time in nanoseconds.
     */
    long nanoTime();

    /**
     * Forces the nanoclock to update the time. The time can be cached. This
     * method should be made after some blocking is done on e.g. a Selector so
     * that the clock remains reasonably up to date.
     */
    void update();
}
