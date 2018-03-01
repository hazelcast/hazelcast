/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

/**
 * This object runs inside a Jet processor, inspects the event timestamps
 * as they occur in the input and and decides on the current value of the
 * watermark. It controls how much disorder Jet will allow in a data stream
 * and which events will be marked as "too late" and dropped. It also
 * decides what to do with the watermark when there are no events for a
 * while. If just one of the many processors working in parallel doesn't
 * receive any events, its watermark may fall behind and impede the
 * progress of the entire DAG. The policy may decide to advance the
 * watermark based on the passage of time alone.
 * <p>
 * The processor must report to this object the timestamp of every event it
 * receives by calling {@link #reportEvent(long)}. It must also ensure that,
 * even in the absence of events, it keeps checking the {@link
 * #getCurrentWatermark() current value} of the watermark. When the
 * watermark advances far enough (as decided by the {@link
 * WatermarkEmissionPolicy}, if in use), it must emit a watermark item to
 * its output edges.
 */
public interface WatermarkPolicy {

    /**
     * Called to report the observation of an event with the given timestamp.
     * Returns the watermark that should be (or have been) emitted before
     * the event.
     * <p>
     * If the returned value is greater than the event's timestamp it means
     * that the event should be dropped.
     *
     * @param timestamp event's timestamp
     * @return the watermark value. May be {@code Long.MIN_VALUE} if there is
     *         insufficient information to determine any watermark (e.g., no events
     *         observed)
     */
    long reportEvent(long timestamp);

    /**
     * Called to get the current watermark in the absence of an observed
     * event. The watermark may advance based just on the passage of time.
     */
    long getCurrentWatermark();

}
