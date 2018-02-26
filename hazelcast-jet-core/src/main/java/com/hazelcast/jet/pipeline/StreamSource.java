/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * Represents an infinite source of data for a Jet pipeline. To aggregate
 * the data from an infinite source, you must specify how to {@link
 * StreamStage#window window} it into finite subsets over which Jet will
 * perform the aggregation.
 *
 * @see Sources source factory methods
 *
 * @param <T> the stream item type
 */
public interface StreamSource<T> {
    /**
     * Returns a descriptive name of this source.
     */
    @Nonnull
    String name();
}
