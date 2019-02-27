/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.function.ToLongFunctionEx;

import javax.annotation.Nonnull;

/**
 * A source stage in a distributed computation {@link Pipeline pipeline}
 * that will observe an unbounded amount of data (i.e., an event stream).
 * Timestamp handling, a prerequisite for attaching data processing stages,
 * is not yet defined in this step. Call one of the methods on this
 * instance to declare whether and how the data source will assign
 * timestamps to events.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface StreamSourceStage<T> {

    /**
     * Declares that the source will not assign any timestamp to the events it
     * emits. You can add them later using {@link
     * GeneralStage#addTimestamps(ToLongFunctionEx, long) addTimestamps},
     * but the behavior is different &mdash; see the note there.
     */
    StreamStage<T> withoutTimestamps();

    /**
     * Declares that the source will assign the time of ingestion as the event
     * timestamp. It will call {@code System.currentTimeMillis()} at the moment
     * it observes an event from the data source and assign it as the event
     * timestamp.
     * <p>
     * <strong>Note:</strong> when snapshotting is enabled to achieve fault
     * tolerance, after a restart Jet replays all the events that were already
     * processed since the last snapshot. These events will then get different
     * timestamps. If you want your job to be fault-tolerant, the events in the
     * stream must have a stable timestamp associated with them. The source may
     * natively provide such timestamps (the {@link #withNativeTimestamps(long)}
     * option). If that is not appropriate, the events should carry their own
     * timestamp as a part of their data and you can use {@link
     * #withTimestamps(ToLongFunctionEx, long)
     * withTimestamps(timestampFn, allowedLag} to extract it.
     * <p>
     * <strong>Note 2:</strong> if the system time goes back (such as when
     * adjusting it), newer events will get older timestamps and might be
     * dropped as late, because the allowed lag is 0.
     */
    default StreamStage<T> withIngestionTimestamps() {
        return withTimestamps(o -> System.currentTimeMillis(), 0);
    }

    /**
     * Declares that the stream will use the source's native timestamps. This
     * is typically the message timestamp that the external system assigns as
     * event's metadata.
     * <p>
     * If there's no notion of native timestamps in the source, this method
     * will throw a {@link JetException}.
     *
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far
     */
    StreamStage<T> withNativeTimestamps(long allowedLag);

    /**
     * Declares that the source will extract timestamps from the stream items.
     *
     * @param timestampFn a function that returns the timestamp for each item, typically in
     *                    milliseconds
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far. The time unit is
     *                   the same as the unit used by {@code timestampFn}
     */
    StreamStage<T> withTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag);
}
