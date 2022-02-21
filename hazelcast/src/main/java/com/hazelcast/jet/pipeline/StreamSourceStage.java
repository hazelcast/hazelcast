/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;

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
 *
 * @since Jet 3.0
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
     * timestamp. The actual time of the original event is ignored.
     * <p>
     * With this mode, unlike {@link #withTimestamps} or {@link
     * #withNativeTimestamps}, the <em>sparse events issue</em> isn't present.
     * You can use this mode to avoid the issue, but there's a caveat:
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
    StreamStage<T> withIngestionTimestamps();

    /**
     * Declares that the stream will use the source's native timestamps. This
     * is typically the message timestamp that the external system assigns as
     * event's metadata.
     * <p>
     * If there's no notion of native timestamps in the source, this method
     * will throw a {@link JetException}.
     * <p>
     * <b>Issue with sparse events</b>
     * <p>
     * Event time progresses only through the ingestion of new events. If the
     * events are sparse, time will effectively stop until a newer event
     * arrives. This causes high latency for time-sensitive operations (such as
     * window aggregation). In addition, Jet tracks event time for every source
     * partition separately, and if just one partition has sparse events, time
     * progress in the whole job is hindered.
     * <p>
     * To overcome this you can either ensure there's a consistent influx of
     * events in every partition, or you can use {@link
     * #withIngestionTimestamps()}.
     *
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far
     */
    StreamStage<T> withNativeTimestamps(long allowedLag);

    /**
     * Declares that the source will extract timestamps from the stream items.
     * <p>
     * <b>Issue with sparse events</b>
     * <p>
     * Event time progresses only through the ingestion of new events. If the
     * events are sparse, time will effectively stop until a newer event
     * arrives. This causes high latency for time-sensitive operations (such as
     * window aggregation). In addition, Jet tracks event time for every source
     * partition separately, and if just one partition has sparse events, time
     * progress in the whole job is hindered.
     * <p>
     * To overcome this you can either ensure there's a consistent influx of
     * events in every partition, or you can use {@link
     * #withIngestionTimestamps()}.
     *
     * @param timestampFn a function that returns the timestamp for each item, typically in
     *                    milliseconds. It must be stateless and {@linkplain
     *                    Processor#isCooperative() cooperative}.
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far. The time unit is
     *                   the same as the unit used by {@code timestampFn}
     */
    StreamStage<T> withTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag);
}
