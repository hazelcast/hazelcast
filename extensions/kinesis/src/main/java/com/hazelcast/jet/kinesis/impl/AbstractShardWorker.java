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

package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.logging.ILogger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

public abstract class AbstractShardWorker {

    protected final KinesisAsyncClient kinesis;
    protected final String streamName;
    protected final ILogger logger;

    protected AbstractShardWorker(KinesisAsyncClient kinesis, String streamName, ILogger logger) {
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.logger = logger;
    }
}
