/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc;

/**
 * Possible Debezium snapshot modes, that does not require additional mandatory parameters to be set.
 */
public enum DebeziumSnapshotMode {

    /**
     * Always perform a snapshot when starting.
     */
    ALWAYS,

    /**
     * Perform a snapshot only upon initial startup of a connector.
     */
    INITIAL,

    /**
     * Never perform a snapshot and only receive logical changes.
     */
    NO_DATA,

    /**
     * Perform a snapshot and then stop before attempting to receive any logical changes.
     */
    INITIAL_ONLY,

    /**
     * Perform a snapshot when it is needed.
     */
    WHEN_NEEDED,

    /**
     * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
     */
    CONFIGURATION_BASED
}
