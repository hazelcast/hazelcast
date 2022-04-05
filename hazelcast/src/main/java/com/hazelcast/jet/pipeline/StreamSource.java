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

import javax.annotation.Nonnull;

/**
 * An infinite source of data for a Jet pipeline. To aggregate the data
 * from an infinite source, you must specify how to {@link
 * StreamStage#window window} it into finite subsets over which Jet will
 * perform the aggregation.
 *
 * @see Sources source factory methods
 *
 * @param <T> the stream item type
 *
 * @since Jet 3.0
 */
public interface StreamSource<T> {
    /**
     * Returns a descriptive name of this source.
     */
    @Nonnull
    String name();

    /**
     * Returns true if this source supports {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamps}.
     */
    boolean supportsNativeTimestamps();

    /**
     * Sets a timeout after which idle partitions will be excluded from
     * watermark coalescing. That is, the source will advance the watermark
     * based on events from other partitions and will ignore the idle
     * partition. If all partitions are idle (or if the source only has one
     * partition), the source will emit a special <em>idle message</em> and the
     * downstream processor will exclude this processor from watermark
     * coalescing.
     * <p>
     * The default timeout is 60 seconds. Must be a positive number or 0 to
     * disable the feature.
     *
     * @param timeoutMillis the timeout in milliseconds or zero to disable.
     *
     * @since Jet 3.1
     */
    StreamSource<T> setPartitionIdleTimeout(long timeoutMillis);

    /**
     * Returns the value set by {@link #setPartitionIdleTimeout(long)}.
     *
     * @since Jet 3.1
     */
    long partitionIdleTimeout();
}
