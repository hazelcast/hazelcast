/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.serialization.BinaryInterface;

import static com.hazelcast.nio.serialization.BinaryInterface.Reason.OTHER_CONVENTION;

/**
 * This exception is thrown when a topology change happens during the
 * execution of a map reduce job and the {@link com.hazelcast.mapreduce.TopologyChangedStrategy}
 * is set to {@link com.hazelcast.mapreduce.TopologyChangedStrategy#CANCEL_RUNNING_OPERATION}.
 *
 * @since 3.2
 * @deprecated This API is superseded by Hazelcast JET (http://jet.hazelcast.org) and will be remove in 3.9
 */
@Deprecated
@BinaryInterface(reason = OTHER_CONVENTION)
public class TopologyChangedException
        extends HazelcastException {

    /**
     * Creates a new instance of the TopologyChangedException
     */
    public TopologyChangedException() {
    }

    /**
     * Creates a new instance of the TopologyChangedException
     *
     * @param message the message to be shown to the user
     */
    public TopologyChangedException(String message) {
        super(message);
    }

}
