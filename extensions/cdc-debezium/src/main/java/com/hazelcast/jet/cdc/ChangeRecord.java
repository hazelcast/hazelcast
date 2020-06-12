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
 * Each event has a <em>key</em>, identifying the affected record, and a
 * <em>value</em>, describing the change to that record.
 * <p>
 * Most events have an <em>operation</em> which specifies the type of
 * change (insertion, delete or update). Events without an operation
 * have specialized usage, for example heartbeats, and aren't supposed
 * to affect the data model. You can observe and act upon them in a Jet
 * CDC sink, but we discourage such usage.
 * <p>
 * All events have a <em>timestamp</em> specifying the moment when the
 * change event occurred in the database. Normally this is the timestamp
 * recorded in the database's change log, but since it has a finite size,
 * the change stream begins with virtual events that reproduce the state of
 * the table at the start of the change log. These events have an
 * artificial timestamp. In principle, it should be easy to identify them
 * because they have a separate {@code SYNC} operation instead of {@code
 * INSERT}, however some databases emit {@code INSERT} events in both
 * cases (a notable example is MySQL).
 *
 * @since 4.2
 */
@EvolvingApi
public interface ChangeRecord {

    /**
     * Specifies the moment when the change event occurred in the database.
     * Normally this is the timestamp recorded in the database's change log,
     * but since it has a finite size, the change stream begins with virtual
     * events that reproduce the state of the table at the start of the change
     * log. These events have an artificial timestamp. In principle, it should
     * be easy to identify them because they have a separate {@code SYNC}
     * operation instead of {@code INSERT}, however some databases emit {@code
     * INSERT} events in both cases (a notable example is MySQL).
     *
     * @throws ParsingException if the timestamp field isn't present or
     *                          is unparsable
     */
    long timestamp() throws ParsingException;

    /**
     * Returns the type of change this record describes (insert, delete or
     * update). Some special events, like heartbeats, don't have an operation
     * value.
     *
     * @return {@link Operation#UNSPECIFIED} if this {@code ChangeRecord}
     * doesn't have an operation field, otherwise the appropriate {@link
     * Operation} that matches the CDC record's operation field
     * @throws ParsingException if there is an operation field, but its
     *                          value is not among the handled ones.
     */
    @Nonnull
    Operation operation() throws ParsingException;

    /**
     * Returns the name of the database containing the record's table.
     *
     * @return name of the source database for the current record
     * @throws ParsingException if the database name field isn't present
     *                          or is unparsable
     */
    @Nonnull
    String database() throws ParsingException;

    /**
     * Returns the name of the table this record is part of.
     *
     * @return name of the source table for the current record
     * @throws ParsingException if the table name field isn't present or
     *                          is unparsable
     */
    @Nonnull
    String table() throws ParsingException;

    /**
     * Returns the key part of the CDC event. It identifies the affected record.
     */
    @Nonnull
    RecordPart key();

    /**
     * Returns the value part of the CDC event. It includes fields like the
     * timestamp, operation, and database record data.
     */
    @Nonnull
    RecordPart value();

    /**
     * Returns the raw JSON string from the CDC event underlying this {@code
     * ChangeRecord}. You can use it if higher-level parsing (see other
     * methods) fails for some reason (for example on some untested combination
     * of database connector and version).
     */
    @Nonnull
    String toJson();
}
