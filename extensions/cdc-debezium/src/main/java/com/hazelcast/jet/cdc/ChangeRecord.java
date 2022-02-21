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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nonnull;

/**
 * Information pertaining to a single data change event (insert, delete or
 * update), affecting a single database record.
 * <p>
 * Each event has a <em>key</em>, identifying the affected record, and a
 * <em>value</em>, describing the change to that record.
 * <p>
 * Most events have an <em>operation</em> which specifies the type of change
 * (insert, delete or update). Events without an operation have specialized
 * usage, for example heartbeats, and aren't supposed to affect the data model.
 * You can observe and act upon them in a Jet CDC sink, but we discourage such
 * usage.
 * <p>
 * All events have a <em>timestamp</em> specifying the moment when the change
 * event occurred in the database. Normally this is the timestamp recorded in
 * the database's change log, but since it has a finite size, the change stream
 * begins with virtual events that reproduce the state of the table at the start
 * of the change log. These events have an artificial timestamp. In principle,
 * it should be easy to identify them because they have a separate {@code SYNC}
 * operation instead of {@code INSERT}, however some databases emit {@code INSERT}
 * events in both cases (a notable example is MySQL).
 * <p>
 * All events have a source-specific <em>sequence</em> which can be used to
 * ensure their ordering. The sequence consists of two parts: a monotonically
 * increasing <em>numeric value</em> and a <em>source descriptor</em>, which
 * provides the scope of validity of the numeric value. This is needed because
 * many CDC sources don't provide a globally valid sequence. For example, the
 * sequence may be the offset in a write-ahead log. Then it makes sense to
 * compare them only if they come from the same log file.
 *
 * @since Jet 4.2
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
     *                          is unparseable
     */
    long timestamp() throws ParsingException;

    /**
     * Specifies the numeric value part of the record's source sequence. As long
     * as the source sequence doesn't change, the values will be monotonically
     * increasing and can be used to impose ordering over the stream of records.
     *
     * @since Jet 4.3
     */
    long sequenceValue();

    /**
     * Specifies the source descriptor of the record's sequence. Any changes
     * observed in its value should be interpreted as a reset in the sequence's
     * numeric values. No ordering can be deduced for two records with different
     * sequence sources.
     *
     * @since Jet 4.3
     */
    long sequenceSource();

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
     *                          or is unparseable
     */
    @Nonnull
    String database() throws ParsingException;

    /**
     * Returns the name of the schema containing the record's table.
     * Note: not all databases have the concept of a schema (for example
     * MySQL).
     *
     * @return name of the source schema for the current record
     * @throws ParsingException if the schema name field isn't present
     *                          or is unparseable
     */
    @Nonnull
    String schema() throws ParsingException, UnsupportedOperationException;

    /**
     * Returns the name of the table this record is part of.
     *
     * @return name of the source table for the current record
     * @throws ParsingException if the table name field isn't present or
     *                          is unparseable
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
     * <p>
     * For <em>insert</em> and <em>update</em> operations the value describes
     * the database record as it looks AFTER the event, so the latest image.
     * <p>
     * For <em>delete</em> operations the value describes the database record as
     * it looked BEFORE the event, so the previous image.
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
