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

package com.hazelcast.internal.config;

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.util.concurrent.Executor;

public class ReliableTopicConfigReadOnly extends ReliableTopicConfig {

    public ReliableTopicConfigReadOnly(ReliableTopicConfig config) {
        super(config);
    }

    @Override
    public ReliableTopicConfig setExecutor(Executor executor) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public ReliableTopicConfig setReadBatchSize(int readBatchSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public ReliableTopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public ReliableTopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public ReliableTopicConfig setTopicOverloadPolicy(TopicOverloadPolicy topicOverloadPolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
