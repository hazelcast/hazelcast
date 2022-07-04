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

package com.hazelcast.jet.cdc.impl;

import java.util.Map;

/**
 * Utility that takes Debezium event headers and computes a sequence
 * number we can use to ensure the ordering of {@code ChangeRecord} items.
 * <p>
 * The <em>sequence</em> part is a monotonically increasing numeric
 * sequence which we base our ordering on.
 * <p>
 * The <em>source</em> part provides the scope of validity of the sequence
 * part. This is needed because many CDC sources don't provide a globally
 * valid sequence. For example, the sequence may be based on transaction
 * IDs, so it makes sense to compare them only if they are produced by the
 * same database instance. Another example is the offset in a write-ahead
 * log, then it makes sense to compare them only if they come from the same
 * log file. Implementations must make sure the sequence is monotonically
 * increasing only across the events with the same source.
 */
public interface SequenceExtractor {

    long source(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset);

    long sequence(Map<String, ?> debeziumOffset);

}
