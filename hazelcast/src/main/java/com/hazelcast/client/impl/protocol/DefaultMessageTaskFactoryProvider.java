/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefSetCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.client.impl.protocol.task.AddBackupListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddPartitionListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxiesMessageTask;
import com.hazelcast.client.impl.protocol.task.DeployClassesMessageTask;
import com.hazelcast.client.impl.protocol.task.IsFailoverSupportedMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.cache.Pre38CacheAddInvalidationListenerTask;
import com.hazelcast.client.impl.protocol.task.cardinality.CardinalityEstimatorAddMessageTask;
import com.hazelcast.client.impl.protocol.task.cardinality.CardinalityEstimatorEstimateMessageTask;
import com.hazelcast.client.impl.protocol.task.crdt.pncounter.PNCounterAddMessageTask;
import com.hazelcast.client.impl.protocol.task.crdt.pncounter.PNCounterGetConfiguredReplicaCountMessageTask;
import com.hazelcast.client.impl.protocol.task.crdt.pncounter.PNCounterGetMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddCacheConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddCardinalityEstimatorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddDurableExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddFlakeIdGeneratorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddListConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddMultiMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddQueueConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReliableTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReplicatedMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddRingbufferConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddScheduledExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddSetConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveAndDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveResultMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ApplyMCConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ChangeClusterStateMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetMemberConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetSystemPropertiesMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetThreadDumpMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetTimedMemberStateMessageTask;
import com.hazelcast.client.impl.protocol.task.management.MatchMCConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.PromoteLiteMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.RunGcMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ShutdownMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.UpdateMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutTransientWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetTtlMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.Pre38MapAddNearCacheEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.metrics.ReadMetricsMessageTask;
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
import com.hazelcast.cp.internal.datastructures.atomiclong.client.AddAndGetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.AlterMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.ApplyMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.CompareAndSetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.GetAndAddMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.GetAndSetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.GetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomicref.client.ContainsMessageTask;
import com.hazelcast.cp.internal.datastructures.atomicref.client.SetMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.AwaitMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.GetCountMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.GetRoundMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.TrySetCountMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.GetLockOwnershipStateMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.TryLockMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.UnlockMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.AcquirePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.AvailablePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.ChangePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.DrainPermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.GetSemaphoreTypeMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.InitSemaphoreMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.ReleasePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.CreateRaftGroupMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.DestroyRaftObjectMessageTask;
import com.hazelcast.cp.internal.session.client.CloseSessionMessageTask;
import com.hazelcast.cp.internal.session.client.CreateSessionMessageTask;
import com.hazelcast.cp.internal.session.client.GenerateThreadIdMessageTask;
import com.hazelcast.cp.internal.session.client.HeartbeatSessionMessageTask;
import com.hazelcast.flakeidgen.impl.client.NewIdBatchMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.longregister.client.codec.LongRegisterAddAndGetCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterDecrementAndGetCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterGetAndAddCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterGetAndIncrementCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterGetAndSetCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterGetCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterIncrementAndGetCodec;
import com.hazelcast.internal.longregister.client.codec.LongRegisterSetCodec;
import com.hazelcast.internal.longregister.client.task.LongRegisterAddAndGetMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterDecrementAndGetMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterGetAndAddMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterGetAndIncrementMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterGetAndSetMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterGetMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterIncrementAndGetMessageTask;
import com.hazelcast.internal.longregister.client.task.LongRegisterSetMessageTask;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.util.MapUtil.createInt2ObjectHashMap;

/**
 * DO NOT EDIT THIS FILE.
 * EDITING THIS FILE CAN POTENTIALLY BREAK PROTOCOL
 */
public class DefaultMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private static final int MESSAGE_TASK_PROVIDER_INITIAL_CAPACITY = 500;

    private final Int2ObjectHashMap<MessageTaskFactory> factories;

    private final Node node;

    public DefaultMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        this.factories = createInt2ObjectHashMap(MESSAGE_TASK_PROVIDER_INITIAL_CAPACITY);
        initFactories();
    }

    public void initFactories() {
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.set
        factories.put(com.hazelcast.client.impl.protocol.codec.SetRemoveListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetContainsAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetAddAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetGetAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetGetAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetAddListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetContainsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetContainsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.SetSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.set.SetSizeMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.ringbuffer
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferReadOneCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadOneMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferCapacityMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferTailSequenceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferTailSequenceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferRemainingCapacityCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadManyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferHeadSequenceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.RingbufferSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferSizeMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cache
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheAssignAndGetUuidsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheReplaceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheContainsKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheCreateConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndReplaceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CachePutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38CacheAddInvalidationListenerTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddNearCacheInvalidationListenerTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CachePutAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheSetExpiryPolicyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheSetExpiryPolicyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheLoadAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheListenerRegistrationMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheDestroyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheEntryProcessorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetAndRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheManagementConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CachePutIfAbsentMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllKeysMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheIterateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.cache.CacheIterateEntriesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalSubscribeTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CacheEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        });

//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.replicatedmap
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsValueCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsValueMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddNearCacheListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapValuesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapValuesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapEntrySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapKeySetMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.internal.longregister.client.task
        factories.put(LongRegisterDecrementAndGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterDecrementAndGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterGetAndAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterGetAndAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterAddAndGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterAddAndGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterIncrementAndGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterIncrementAndGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterGetAndSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterGetAndSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(LongRegisterGetAndIncrementCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LongRegisterGetAndIncrementMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionallist
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalListSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalListRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalListAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListAddMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmultimap
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.list
        factories.put(com.hazelcast.client.impl.protocol.codec.ListGetAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListListIteratorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListListIteratorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListAddAllWithIndexCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllWithIndexMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveWithIndexMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListIteratorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIteratorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListAddAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListAddWithIndexCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListAddWithIndexMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListLastIndexOfMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListSubCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSubMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListContainsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListIndexOfCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIndexOfMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListContainsAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListContainsAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ListCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.list.ListCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalqueue
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueOfferMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePeekMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePollMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueTakeMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.multimap
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapTryLockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapIsLockedMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsValueMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapKeySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapEntrySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValueCountMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapUnlockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapLockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsEntryMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapForceUnlockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapValuesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MultiMapDeleteCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.multimap.MultiMapDeleteMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.topic
        factories.put(com.hazelcast.client.impl.protocol.codec.TopicPublishCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicPublishMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.topic.TopicRemoveMessageListenerMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalmap
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetForUpdateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapContainsKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapContainsKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapDeleteCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapDeleteMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.executorservice
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceIsShutdownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceShutdownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.durableexecutor
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorIsShutdownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveResultMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorDisposeResultMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DurableExecutorRetrieveAndDisposeResultMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transaction
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCreateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAClearRemoteTransactionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionFinalizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XAFinalizeTransactionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionCommitMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XACollectTransactionsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionPrepareCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionPrepareMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionCreateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCreateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.TransactionRollbackMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionCommitCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionCommitMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.XATransactionRollbackCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transaction.XATransactionRollbackMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.transactionalset
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalSetSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalSetAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.TransactionalSetRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetRemoveMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.map
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAssignAndGetUuidsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveIfSameMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddInterceptorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutTransientMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsValueMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapReplaceCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveInterceptorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new Pre38MapAddNearCacheEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddNearCacheInvalidationListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnAllKeysMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapFlushCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFlushMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapTryLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryLockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEntrySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapGetEntryViewCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetEntryViewMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadGivenKeysMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapUnlockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapUnlockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEvictCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapGetAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapForceUnlockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapLoadAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapLoadAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddIndexMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveEntryListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapIsLockedCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapIsLockedMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapEvictAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSubmitToKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapValuesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapDeleteCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDeleteMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPutAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeysMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapContainsKeyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapTryPutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapTryPutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapValuesWithPagingPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapKeySetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapKeySetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchKeysMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchEntriesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAggregateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapAggregateWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapProjectCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapProjectionWithPredicateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapFetchWithQueryMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalSubscribeTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapEventJournalReadCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapEventJournalReadTask<Object, Object, Object>(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapSetTtlCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapSetTtlMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapSetWithMaxIdleCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapSetWithMaxIdleMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutWithMaxIdleCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapPutWithMaxIdleMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentWithMaxIdleCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapPutIfAbsentWithMaxIdleMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MapPutTransientWithMaxIdleCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new MapPutTransientWithMaxIdleMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientPingCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.PingMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DeployClassesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddPartitionListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientCreateProxiesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateProxiesMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientIsFailoverSupportedCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new IsFailoverSupportedMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddBackupListenerMessageTask(clientMessage, node, connection);
            }
        });

//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.queue
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRemoveAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueAddAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueTakeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueTakeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueAddListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueAddListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRetainAllMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueOfferCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueOfferMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueuePeekCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePeekMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueRemoveCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIsEmptyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueIteratorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueIteratorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueSizeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueuePutCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePutMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueContainsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueContainsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueuePollCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueuePollMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueDrainToCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemoveListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueRemainingCapacityMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueClearCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueClearMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.queue.QueueDrainMaxSizeMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.cardinality
        factories.put(com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CardinalityEstimatorEstimateMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.scheduledexecutor
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorSubmitToAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorShutdownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskDisposeFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskCancelFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsDoneFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetDelayFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetStatisticsFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskGetResultFromAddressMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorGetAllScheduledMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromAddressCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ScheduledExecutorTaskIsCancelledFromAddressMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR continuous query operations of com.hazelcast.client.impl.protocol.task.map
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapDestroyCacheMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapSetReadCursorMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQueryAddListenerCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapAddListenerMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapMadePublishableMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateWithValueMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR dynamic config configuration
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMultiMapConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCardinalityEstimatorConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddExecutorConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddDurableExecutorConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddScheduledExecutorConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddRingbufferConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddListConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddListConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSetConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddTopicConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReplicatedMapConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddQueueConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddMapConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddReliableTopicConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddCacheConfigMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.DynamicConfigAddFlakeIdGeneratorConfigCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddFlakeIdGeneratorConfigMessageTask(clientMessage, node, connection);
            }
        });
//endregion
// region ----------- REGISTRATION FOR flake id generator
        factories.put(com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new NewIdBatchMessageTask(clientMessage, node, connection);
            }
        });
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.crdt.pncounter
        factories.put(com.hazelcast.client.impl.protocol.codec.PNCounterGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PNCounterGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PNCounterAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.PNCounterGetConfiguredReplicaCountCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PNCounterGetConfiguredReplicaCountMessageTask(clientMessage, node, connection);
            }
        });
//endregion


        factories.put(com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateRaftGroupMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyRaftObjectMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateSessionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new HeartbeatSessionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CloseSessionMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GenerateThreadIdMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(AtomicLongAddAndGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddAndGetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongCompareAndSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CompareAndSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongGetAndAddCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndAddMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongGetAndSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndSetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongApplyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ApplyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicLongAlterCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AlterMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(AtomicRefApplyCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.cp.internal.datastructures.atomicref.client.ApplyMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicRefSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new SetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicRefContainsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ContainsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicRefGetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.cp.internal.datastructures.atomicref.client.GetMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(AtomicRefCompareAndSetCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.cp.internal.datastructures.atomicref.client.CompareAndSetMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(CountDownLatchAwaitCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AwaitMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(CountDownLatchCountDownCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CountDownMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(CountDownLatchGetCountCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetCountMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(CountDownLatchGetRoundCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetRoundMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(CountDownLatchTrySetCountCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TrySetCountMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(FencedLockLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(FencedLockTryLockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TryLockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(FencedLockUnlockCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new UnlockMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(FencedLockGetLockOwnershipCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetLockOwnershipStateMessageTask(clientMessage, node, connection);
            }
        });

        factories.put(SemaphoreAcquireCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AcquirePermitsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreAvailablePermitsCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AvailablePermitsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreChangeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ChangePermitsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreDrainCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DrainPermitsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreGetSemaphoreTypeCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetSemaphoreTypeMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreInitCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new InitSemaphoreMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(SemaphoreReleaseCodec.REQUEST_MESSAGE_TYPE, new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ReleasePermitsMessageTask(clientMessage, node, connection);
            }
        });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    @Override
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new ReadMetricsMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new ChangeClusterStateMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new GetMapConfigMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new UpdateMapConfigMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCGetMemberConfigCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new GetMemberConfigMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCRunGcCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new RunGcMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCGetThreadDumpCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new GetThreadDumpMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new ShutdownMemberMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new PromoteLiteMemberMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCGetSystemPropertiesCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new GetSystemPropertiesMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new GetTimedMemberStateMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCMatchMCConfigCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new MatchMCConfigMessageTask(clientMessage, node, connection);
                    }
                });
        factories.put(com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec.REQUEST_MESSAGE_TYPE,
                new MessageTaskFactory() {
                    public MessageTask create(ClientMessage clientMessage, Connection connection) {
                        return new ApplyMCConfigMessageTask(clientMessage, node, connection);
                    }
                });
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}


