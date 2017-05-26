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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.task.DeployClassesMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.cache.Pre38CacheAddInvalidationListenerTask;
import com.hazelcast.client.impl.protocol.task.cardinality.CardinalityEstimatorAddMessageTask;
import com.hazelcast.client.impl.protocol.task.cardinality.CardinalityEstimatorEstimateMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddCacheConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddCardinalityEstimatorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddDurableExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddListConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddLockConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddMultiMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddQueueConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReliableTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReplicatedMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddRingbufferConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddScheduledExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddSemaphoreConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddSetConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveAndDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveResultMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.Pre38MapAddNearCacheEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorGetAllScheduledMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromPartitionMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * DO NOT EDIT THIS FILE.
 * EDITING THIS FILE CAN POTENTIALLY BREAK PROTOCOL
 */
public class DefaultMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];


    private final Node node;

    public DefaultMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.set
        factories[com.hazelcast.client.impl.protocol.codec.SetRemoveListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetCompareAndRemoveAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetContainsAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetCompareAndRetainAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetGetAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetContainsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.ringbuffer
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferReadOneCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadOneMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferTailSequenceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferTailSequenceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferRemainingCapacityCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadManyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferHeadSequenceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.lock
        factories[com.hazelcast.client.impl.protocol.codec.LockUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockIsLockedCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockForceUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockGetRemainingLeaseTimeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockGetRemainingLeaseTimeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockIsLockedByCurrentThreadCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockIsLockedByCurrentThreadMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockTryLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockGetLockCountCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockGetLockCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cache
        factories[com.hazelcast.client.impl.protocol.codec.CacheClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAssignAndGetUuidsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheCreateConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38CacheAddInvalidationListenerTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddNearCacheInvalidationListenerTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheLoadAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheListenerRegistrationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheDestroyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheEntryProcessorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheManagementConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheIterateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateEntriesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalSubscribeTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        };

//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.mapreduce
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceJobProcessInformationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceJobProcessInformationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceCancelCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceCancelMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForCustomCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForCustomMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForMapCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMapMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForListCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForListMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForMultiMapCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMultiMapMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.replicatedmap
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsValueCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddNearCacheListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapValuesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.atomiclong
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongApplyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongDecrementAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAlterAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAddAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongCompareAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongIncrementAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndIncrementMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.semaphore
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreDrainPermitsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreDrainPermitsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreAvailablePermitsMessageTasks(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreInitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreAcquireMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreReducePermitsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreReducePermitsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreTryAcquireCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreTryAcquireMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreReleaseMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionallist
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListAddMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmultimap
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.condition
        factories[com.hazelcast.client.impl.protocol.codec.ConditionSignalCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionSignalMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionBeforeAwaitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionBeforeAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionAwaitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionSignalAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionSignalAllMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.list
        factories[com.hazelcast.client.impl.protocol.codec.ListGetAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListListIteratorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListListIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddAllWithIndexCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListCompareAndRemoveAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIteratorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddWithIndexCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListLastIndexOfMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSubCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSubMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListContainsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIndexOfCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIndexOfMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListContainsAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListCompareAndRetainAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.countdownlatch
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchCountDownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchGetCountMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchTrySetCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalqueue
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueOfferMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePeekMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePollMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueTakeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.multimap
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValuesMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.atomicreference
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceCompareAndSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceCompareAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndAlterCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetAndAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceApplyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceApplyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceIsNullCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceIsNullMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceAlterAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetAndGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceSetAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceContainsMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.topic
        factories[com.hazelcast.client.impl.protocol.codec.TopicPublishCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicPublishMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicRemoveMessageListenerMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmap
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetForUpdateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapContainsKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapDeleteCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapDeleteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.executorservice
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceIsShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.durableexecutor
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorIsShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveResultMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorDisposeResultMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveAndDisposeResultMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transaction
        factories[com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAClearRemoteTransactionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionFinalizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAFinalizeTransactionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCommitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XACollectTransactionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionPrepareCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionPrepareMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCreateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionRollbackMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCommitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCommitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionRollbackCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionRollbackMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalset
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetRemoveMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.map
        factories[com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapClearNearCacheCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapClearNearCacheMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAssignAndGetUuidsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddInterceptorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutTransientMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReplaceCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveInterceptorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38MapAddNearCacheEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddNearCacheInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnAllKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFlushCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFlushMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetEntryViewCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetEntryViewMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadGivenKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEvictCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLoadAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapIsLockedCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSubmitToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapDeleteCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDeleteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryPutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchEntriesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAggregateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapProjectCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchWithQueryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalSubscribeTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEventJournalReadCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientPingCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.PingMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DeployClassesMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.queue
        factories[com.hazelcast.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueAddAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueTakeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueTakeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueAddListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueCompareAndRetainAllCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueOfferCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueOfferMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePeekCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePeekMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemoveCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueIsEmptyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueIteratorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePutCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueContainsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePollCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePollMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueDrainToCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemoveListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemainingCapacityCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueClearCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMaxSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cardinality
        factories[com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorEstimateMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.scheduledexecutor
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorGetAllScheduledMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromAddressCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromAddressMessageTask(clientMessage, node, connection);
            }
        };
//endregion

        //region ----------  REGISTRATION FOR continuous query operations of com.hazelcast.client.impl.protocol.task.map
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDestroyCacheMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetReadCursorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryAddListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapMadePublishableMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateWithValueMessageTask(clientMessage, node, connection);
            }
        };
//endregion
        //region ----------  REGISTRATION FOR dynamic config configuration
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMultiMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCardinalityEstimatorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddDurableExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddScheduledExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddRingbufferConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddLockConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddLockConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddListConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddListConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSetConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSemaphoreConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSemaphoreConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddTopicConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReplicatedMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddQueueConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReliableTopicConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCacheConfigMessageTask(clientMessage, node, connection);
            }
        };
//endregion
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public MessageTaskFactory[] getFactories() {
        return factories;
    }
}


