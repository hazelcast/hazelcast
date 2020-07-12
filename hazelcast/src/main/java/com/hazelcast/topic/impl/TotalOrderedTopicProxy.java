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

import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newDelegatingFuture;

/**
 * Topic proxy used when global ordering is enabled (all nodes listening to
 * the same topic get their messages in the same order).
 *
 * @param <E> the type of message in this topic
 */
public class TotalOrderedTopicProxy<E> extends TopicProxy<E> {

    private final OperationService operationService;
    private final SerializationService serializationService;
    private final int partitionId;
    private final LocalTopicStatsImpl topicStats;

    public TotalOrderedTopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
        this.partitionId = nodeEngine.getPartitionService()
                .getPartitionId(getNameAsPartitionAwareData());
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.topicStats = service.getLocalTopicStats(name);
    }

    @Override
    public void publish(@Nonnull  E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);
        Operation operation = new PublishOperation(getName(), toData(message))
                .setPartitionId(partitionId);
        InternalCompletableFuture f = invokeOnPartition(operation);
        f.joinInternal();
    }

    @Override
    public CompletionStage<E> publishAsync(@Nonnull E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);
        Operation op = new PublishOperation(getName(), toData(message))
                .setPartitionId(partitionId);
        return newDelegatingFuture(serializationService, publishInternalAsync(op));
    }

    @Override
    public void publishAll(@Nonnull Collection<? extends E> messages) {
        checkNotNull(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(messages, NULL_MESSAGE_IS_NOT_ALLOWED);

        Operation op = new PublishAllOperation(getName(), toDataArray(messages));
        publishInternalAsync(op).joinInternal();
    }

    @Override
    public CompletionStage<E> publishAllAsync(@Nonnull Collection<? extends E> messages) {
        checkNotNull(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(messages, NULL_MESSAGE_IS_NOT_ALLOWED);

        Operation op = new PublishAllOperation(getName(), toDataArray(messages));
        return newDelegatingFuture(serializationService, publishInternalAsync(op));
    }

    private InternalCompletableFuture<Data> publishInternalAsync(Operation operation) {
        topicStats.incrementPublishes();
        try {
            InvocationFuture<Data> future = operationService.invokeOnPartition(OperationService.SERVICE_NAME,
                    operation, partitionId);
            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
