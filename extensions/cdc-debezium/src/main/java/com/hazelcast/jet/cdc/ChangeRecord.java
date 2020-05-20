/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nonnull;

/**
 * Information pertaining to a single data change event (insertion,
 * delete or update), affecting a single database record.
 * <p>
 * Each event has a <i>key</i>, which identifies the particular record
 * being affected, and a <i>value</i>, which describes the actual change
 * itself.
 * <p>
 * Most events have an <i>operation</i> associated with them which
 * specifies the type of change being described (insertion, delete or
 * update). Only some special events, like heartbeats don't have an
 * operation value. (Heartbeat events are not something we encourage the
 * usage of, but since no functionality of the underlying Debezium
 * connectors is disabled they are still theoretically possible to
 * enable and be observed in Jet.)
 * <p>
 * There is also a <i>timestamp</i> which specifies the moment in time
 * when the event happened. This timestamp is actual event time. It
 * originates in the database change-log, so it's not "processing time",
 * ie. not the moment when the event was observed by our system. Keep in
 * mind though that not all events come from the change-log. The
 * change-log goes back in time only to a limited extent, all older
 * events are parts of a database snapshot constructed when we start
 * monitoring the database and their timestamps are accordingly
 * artificial. Identifying snapshot events is possible most of the time,
 * because their operation will be {@link Operation#SYNC} instead of
 * {@link Operation#INSERT} (one notable exception being MySQL).
 *
 * @since 4.2
 */
@EvolvingApi
public interface ChangeRecord {

    /**
     * Specifies the moment in time when the event happened. This
     * timestamp is actual event time. It originates in the database
     * change-log, so it's not "processing time", ie. not the moment
     * when the event was observed by our system. Keep in mind though
     * that not all events come from the change-log. The change-log goes
     * back in time only to a limited extent, all older events are parts
     * of a database snapshot constructed when we start monitoring the
     * database and their timestamps are accordingly artificial.
     * Identifying snapshot events is possible most of the time, because
     * their operation will be {@link Operation#SYNC} instead of
     * {@link Operation#INSERT} (one notable exception being MySQL).
     *
     * @throws ParsingException if no parsable timestamp field present
     */
    long timestamp() throws ParsingException;

    /**
     * Specifies the type of change being described (insertion, delete or
     * update). Only some special events, like heartbeats don't have an
     * operation value.
     *
     * @return {@link Operation#UNSPECIFIED} if this {@code ChangeRecord}
     * doesn't have an operation field or appropriate {@link Operation}
     * that matches what's found in the operation field
     * @throws ParsingException if there is an operation field, but it's
     *                          value is not among the handled ones.
     */
    @Nonnull
    Operation operation() throws ParsingException;

    /**
     * Identifies the particular record being affected by the change
     * event.
     */
    @Nonnull
    RecordPart key();

    /**
     * Describes the actual change affected on the record by the change
     * event.
     */
    @Nonnull
    RecordPart value();

    /**
     * Returns raw JSON string which the content of this event is
     * based on. Mean to be used when higher level parsing (see other
     * methods) fails for some reason (for example on some untested
     * DB-connector version combination).
     */
    @Nonnull
    String toJson();
}
