/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.memory;

import com.hazelcast.jet.config.InstanceConfig;

/**
 * This exception is thrown when a job exceeds a configurable processor accumulation limit.
 *
 * @see InstanceConfig#setMaxProcessorAccumulatedRecords
 */
public class AccumulationLimitExceededException extends RuntimeException {

    public AccumulationLimitExceededException() {
        super("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance."
                + " An OOME might occur when a job accumulates large data sets in one of the processors,"
                + " e.g. grouping, sorting, hash join."
                + "See InstanceConfig.setMaxProcessorAccumulatedRecords() for further details.");
    }
}
