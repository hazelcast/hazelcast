/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic.impl;

import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Topic proxy used when global ordering is enabled (all nodes listening to
 * the same topic get their messages in the same order).
 *
 * @param <E> the type of message in this topic
 */
public class TotalOrderedTopicProxy<E> extends TopicProxy<E> {

    private final int partitionId;

    public TotalOrderedTopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void publish(@Nonnull  E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);
        Operation operation = new PublishOperation(getName(), toData(message))
                .setPartitionId(partitionId);
        InternalCompletableFuture f = invokeOnPartition(operation);
        f.joinInternal();
    }
}
