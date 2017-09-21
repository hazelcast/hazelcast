/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

/**
 * A policy object that decides on the watermark in a single data
 * (sub)stream. The timestamp of every observed item should be reported
 * to this object and it will respond with the current value of the
 * watermark. Watermark may also advance in the absence of observed
 * events; {@link #getCurrentWatermark()} can be called at any
 * time to see this change.
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
