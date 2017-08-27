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

import com.hazelcast.client.impl.protocol.task.AddPartitionListenerMessageTask;
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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddEventJournalConfigMessageTask;
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
        factories[com.hazelcast.client.impl.protocol.codec.SetRemoveListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetCompareAndRemoveAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetContainsAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetCompareAndRetainAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetGetAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetContainsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SetSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.ringbuffer
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferReadOneCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadOneMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferTailSequenceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferTailSequenceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferRemainingCapacityCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadManyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferHeadSequenceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.RingbufferSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.lock
        factories[com.hazelcast.client.impl.protocol.codec.LockUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockIsLockedCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockForceUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockGetRemainingLeaseTimeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockGetRemainingLeaseTimeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockIsLockedByCurrentThreadCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockIsLockedByCurrentThreadMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockTryLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.LockGetLockCountCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.lock.LockGetLockCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cache
        factories[com.hazelcast.client.impl.protocol.codec.CacheClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAssignAndGetUuidsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheCreateConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38CacheAddInvalidationListenerTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddNearCacheInvalidationListenerTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheLoadAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheListenerRegistrationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheDestroyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheEntryProcessorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheManagementConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheIterateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateEntriesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalSubscribeTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        };

//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.mapreduce
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceJobProcessInformationCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceJobProcessInformationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceCancelCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceCancelMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForCustomCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForCustomMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForMapCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMapMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForListCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForListMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReduceForMultiMapCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMultiMapMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.replicatedmap
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsValueCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddNearCacheListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapValuesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.atomiclong
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongApplyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongDecrementAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAlterAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAddAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongCompareAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongIncrementAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomiclong.AtomicLongGetAndIncrementMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.semaphore
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreDrainPermitsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreDrainPermitsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreAvailablePermitsMessageTasks(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreInitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreAcquireMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreReducePermitsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreReducePermitsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreTryAcquireCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreTryAcquireMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.semaphore.SemaphoreReleaseMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionallist
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalListAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListAddMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmultimap
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.condition
        factories[com.hazelcast.client.impl.protocol.codec.ConditionSignalCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionSignalMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionBeforeAwaitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionBeforeAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionAwaitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ConditionSignalAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.condition.ConditionSignalAllMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.list
        factories[com.hazelcast.client.impl.protocol.codec.ListGetAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListListIteratorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListListIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddAllWithIndexCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListCompareAndRemoveAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIteratorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListAddWithIndexCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddWithIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListLastIndexOfMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSubCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSubMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListContainsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIndexOfCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIndexOfMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListContainsAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ListCompareAndRetainAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.countdownlatch
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchAwaitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchCountDownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchGetCountMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.countdownlatch.CountDownLatchTrySetCountMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalqueue
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueOfferMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePeekMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePollMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueTakeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.multimap
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsEntryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValuesMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.atomicreference
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceCompareAndSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceCompareAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndAlterCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetAndAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceGetAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceApplyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceApplyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceIsNullCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceIsNullMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceAlterAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetAndGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceSetAndGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceAlterMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.atomicreference.AtomicReferenceContainsMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.topic
        factories[com.hazelcast.client.impl.protocol.codec.TopicPublishCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicPublishMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicRemoveMessageListenerMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmap
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetForUpdateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapContainsKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapDeleteCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapDeleteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.executorservice
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceIsShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.durableexecutor
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorIsShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveResultMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorDisposeResultMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveAndDisposeResultMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transaction
        factories[com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAClearRemoteTransactionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionFinalizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAFinalizeTransactionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCommitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XACollectTransactionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionPrepareCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionPrepareMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCreateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionRollbackMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionCommitCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCommitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.XATransactionRollbackCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionRollbackMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalset
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.TransactionalSetRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetRemoveMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.map
        factories[com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapClearNearCacheCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapClearNearCacheMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAssignAndGetUuidsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddInterceptorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutTransientMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsValueMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReplaceCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveInterceptorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38MapAddNearCacheEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddNearCacheInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnAllKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFlushCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFlushMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntrySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetEntryViewCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetEntryViewMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadGivenKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEvictCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapForceUnlockMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapLoadAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddIndexMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapIsLockedCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsLockedMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSubmitToKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapDeleteCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDeleteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapPutAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsKeyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapTryPutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryPutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapGetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapKeySetCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchKeysMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchEntriesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAggregateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapProjectCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionWithPredicateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchWithQueryMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalSubscribeTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.MapEventJournalReadCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientPingCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.PingMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DeployClassesMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddPartitionListenerMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.queue
        factories[com.hazelcast.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueAddAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueTakeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueTakeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueAddListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueCompareAndRetainAllCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueOfferCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueOfferMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePeekCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePeekMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemoveCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueIsEmptyCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIsEmptyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueIteratorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIteratorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueSizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePutCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePutMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueContainsCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueuePollCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePollMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueDrainToCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemoveListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueRemainingCapacityCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueClearCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueClearMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMaxSizeMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cardinality
        factories[com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorAddMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorEstimateMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.scheduledexecutor
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromAddressMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorGetAllScheduledMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromAddressCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromAddressMessageTask(clientMessage, node, connection);
            }
        };
//endregion

        //region ----------  REGISTRATION FOR continuous query operations of com.hazelcast.client.impl.protocol.task.map
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDestroyCacheMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetReadCursorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryAddListenerCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapMadePublishableMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateWithValueMessageTask(clientMessage, node, connection);
            }
        };
//endregion
        //region ----------  REGISTRATION FOR dynamic config configuration
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMultiMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCardinalityEstimatorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddDurableExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddScheduledExecutorConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddRingbufferConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddLockConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddLockConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddListConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddListConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSetConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSemaphoreConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSemaphoreConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddTopicConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReplicatedMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddQueueConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMapConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReliableTopicConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec.REQUEST_TYPE] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCacheConfigMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.DynamicConfigAddEventJournalConfigCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddEventJournalConfigMessageTask(clientMessage, node, connection);
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


