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
package com.hazelcast.jet.pulsar;

/**
 * Metrics Name for Apache Pulsar connector
 */
public final class PulsarMetricNames {

    /**
     * How many partitions are assigned to given processor.
      */
    public static final String ASSIGNED_PARTITION_COUNT = "assignedPartitionsCount";

    /**
     * How many messages are "in flight" (sent, but not yet delivered) in given moment.
      */
    public static final String IN_FLIGHT_MESSAGES = "inFlightMessages";

    private PulsarMetricNames() {
    }
}
