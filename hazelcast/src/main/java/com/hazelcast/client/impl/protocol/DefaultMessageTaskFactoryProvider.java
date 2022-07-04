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
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheIterateCodec;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutAllCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CacheSetExpiryPolicyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheSizeCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddMigrationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxiesCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec;
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientFetchSchemaCodec;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveMigrationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSendAllSchemasCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSendSchemaCodec;
import com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec;
import com.hazelcast.client.impl.protocol.codec.ClientTriggerPartitionAssignmentCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddFlakeIdGeneratorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddListConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddPNCounterConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddAllWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListClearCodec;
import com.hazelcast.client.impl.protocol.codec.ListCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListContainsCodec;
import com.hazelcast.client.impl.protocol.codec.ListGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListGetCodec;
import com.hazelcast.client.impl.protocol.codec.ListIndexOfCodec;
import com.hazelcast.client.impl.protocol.codec.ListIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.ListIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec;
import com.hazelcast.client.impl.protocol.codec.ListListIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListSetCodec;
import com.hazelcast.client.impl.protocol.codec.ListSizeCodec;
import com.hazelcast.client.impl.protocol.codec.ListSubCodec;
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetCPMembersCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMemberConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetThreadDumpCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCMatchMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCPollMCEventsCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteToCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.MCReloadConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetQueueAgeStatisticsCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunGcCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownClusterCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerForceStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MapEventJournalReadCodec;
import com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.impl.protocol.codec.MapFlushCodec;
import com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetEntryViewCodec;
import com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.MapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MapProjectCodec;
import com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetTtlCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetConfiguredReplicaCountCodec;
import com.hazelcast.client.impl.protocol.codec.QueueAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.QueueClearCodec;
import com.hazelcast.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueContainsCodec;
import com.hazelcast.client.impl.protocol.codec.QueueDrainToCodec;
import com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec;
import com.hazelcast.client.impl.protocol.codec.QueueIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.QueueIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.QueueOfferCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePeekCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePollCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePutCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemainingCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.QueueSizeCodec;
import com.hazelcast.client.impl.protocol.codec.QueueTakeCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferReadOneCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferRemainingCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferSizeCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferTailSequenceCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.SetClearCodec;
import com.hazelcast.client.impl.protocol.codec.SetCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetContainsCodec;
import com.hazelcast.client.impl.protocol.codec.SetGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.SetRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.SetRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.SetSizeCodec;
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.protocol.codec.SqlMappingDdlCodec;
import com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec;
import com.hazelcast.client.impl.protocol.codec.TopicPublishAllCodec;
import com.hazelcast.client.impl.protocol.codec.TopicPublishCodec;
import com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalListAddCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalListRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalListSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapSetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueOfferCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePeekCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueuePollCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalQueueTakeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetAddCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetSizeCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionCommitCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionCreateCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionFinalizeCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionPrepareCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionRollbackCodec;
import com.hazelcast.client.impl.protocol.task.AddBackupListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddClusterViewListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddMigrationListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxiesMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask;
import com.hazelcast.client.impl.protocol.task.DeployClassesMessageTask;
import com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask;
import com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask;
import com.hazelcast.client.impl.protocol.task.PingMessageTask;
import com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.RemoveMigrationListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.TriggerPartitionAssignmentMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheAddEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheAddNearCacheInvalidationListenerTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheAddPartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheClearMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheCreateConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheDestroyMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEntryProcessorMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheGetAllMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheGetAndRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheGetAndReplaceMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheGetConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheGetMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheIterateEntriesMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheIterateMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheListenerRegistrationMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheLoadAllMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheManagementConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CachePutAllMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CachePutIfAbsentMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CachePutMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllKeysMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemoveAllMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemoveEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemoveInvalidationListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheRemovePartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheReplaceMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheSetExpiryPolicyMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheSizeMessageTask;
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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddPNCounterConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddQueueConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReliableTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddReplicatedMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddRingbufferConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddScheduledExecutorConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddSetConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AddTopicConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceIsShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToAddressMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorIsShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveAndDisposeResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveResultMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.executorservice.durable.DurableExecutorSubmitToPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListAddAllMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListAddAllWithIndexMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListAddListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListAddMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListAddWithIndexMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListClearMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListCompareAndRemoveAllMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListCompareAndRetainAllMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListContainsAllMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListContainsMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListGetAllMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListGetMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListIndexOfMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListIteratorMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListLastIndexOfMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListListIteratorMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListRemoveListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListRemoveWithIndexMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListSetMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.list.ListSubMessageTask;
import com.hazelcast.client.impl.protocol.task.management.AddWanBatchPublisherConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ApplyClientFilteringConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ChangeClusterStateMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ChangeClusterVersionMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ChangeWanReplicationStateMessageTask;
import com.hazelcast.client.impl.protocol.task.management.CheckWanConsistencyMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ClearWanQueuesMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetCPMembersMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetClusterMetadataMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetMemberConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetSystemPropertiesMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetThreadDumpMessageTask;
import com.hazelcast.client.impl.protocol.task.management.GetTimedMemberStateMessageTask;
import com.hazelcast.client.impl.protocol.task.management.HotRestartInterruptBackupMessageTask;
import com.hazelcast.client.impl.protocol.task.management.HotRestartTriggerBackupMessageTask;
import com.hazelcast.client.impl.protocol.task.management.HotRestartTriggerForceStartMessageTask;
import com.hazelcast.client.impl.protocol.task.management.HotRestartTriggerPartialStartMessageTask;
import com.hazelcast.client.impl.protocol.task.management.MatchClientFilteringConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.PollMCEventsMessageTask;
import com.hazelcast.client.impl.protocol.task.management.PromoteLiteMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.PromoteToCPMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.QueueResetAgeStatisticsMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ReloadConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.RemoveCPMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ResetCPSubsystemMessageTask;
import com.hazelcast.client.impl.protocol.task.management.RunConsoleCommandMessageTask;
import com.hazelcast.client.impl.protocol.task.management.RunGcMessageTask;
import com.hazelcast.client.impl.protocol.task.management.RunScriptMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ShutdownClusterMessageTask;
import com.hazelcast.client.impl.protocol.task.management.ShutdownMemberMessageTask;
import com.hazelcast.client.impl.protocol.task.management.UpdateConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.UpdateMapConfigMessageTask;
import com.hazelcast.client.impl.protocol.task.management.WanSyncMapMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerToKeyWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddEntryListenerWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddIndexMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddInterceptorMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddNearCacheInvalidationListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddPartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAggregateWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapClearMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapContainsValueMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapDeleteMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapDestroyCacheMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPagingPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEntriesWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEntrySetMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalReadTask;
import com.hazelcast.client.impl.protocol.task.map.MapEventJournalSubscribeTask;
import com.hazelcast.client.impl.protocol.task.map.MapEvictAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapEvictMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapExecuteOnAllKeysMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapExecuteOnKeysMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapExecuteWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapFetchEntriesMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapFetchKeysMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask;
import com.hazelcast.client.impl.protocol.task.map.MapFetchWithQueryMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapFlushMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapForceUnlockMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapGetAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapGetEntryViewMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapGetMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapIsLockedMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapKeySetMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPagingPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapKeySetWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapLoadAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapLoadGivenKeysMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapLockMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapMadePublishableMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapProjectionWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPublisherCreateWithValueMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutIfAbsentWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutTransientMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutTransientWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapPutWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemoveAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemoveEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemoveIfSameMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemoveInterceptorMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapRemovePartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapReplaceAllMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapReplaceIfSameMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapReplaceMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetReadCursorMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetTtlMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSetWithMaxIdleMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapSubmitToKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapTryLockMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapTryPutMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapTryRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapUnlockMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapValuesMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapValuesWithPagingPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapValuesWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.metrics.ReadMetricsMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapAddEntryListenerToKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapClearMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsEntryMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapContainsValueMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapDeleteMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapEntrySetMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapForceUnlockMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapGetMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapIsLockedMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapKeySetMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapLockMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapPutAllMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveEntryMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapTryLockMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapUnlockMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapValueCountMessageTask;
import com.hazelcast.client.impl.protocol.task.multimap.MultiMapValuesMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueAddAllMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueAddListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueClearMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRemoveAllMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueCompareAndRetainAllMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueContainsAllMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueContainsMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueDrainMaxSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueDrainMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueIteratorMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueOfferMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueuePeekMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueuePollMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueuePutMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueRemainingCapacityMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueRemoveListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.queue.QueueTakeMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapAddNearCacheListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapClearMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsValueMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapEntrySetMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapGetMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapKeySetMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.replicatedmap.ReplicatedMapValuesMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddAllMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferAddMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferCapacityMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferHeadSequenceMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadManyMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferReadOneMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferRemainingCapacityMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.ringbuffer.RingbufferTailSequenceMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorGetAllScheduledMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorShutdownMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromTargetMessageTask;
import com.hazelcast.client.impl.protocol.task.schema.FetchSchemaMessageTask;
import com.hazelcast.client.impl.protocol.task.schema.SendAllSchemasMessageTask;
import com.hazelcast.client.impl.protocol.task.schema.SendSchemaMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetAddAllMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetAddListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetAddMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetClearMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetCompareAndRemoveAllMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetCompareAndRetainAllMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetContainsAllMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetContainsMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetGetAllMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetRemoveListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.set.SetSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.topic.TopicPublishAllMessageTask;
import com.hazelcast.client.impl.protocol.task.topic.TopicPublishMessageTask;
import com.hazelcast.client.impl.protocol.task.topic.TopicRemoveMessageListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.TransactionCommitMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.TransactionCreateMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.TransactionRollbackMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XAClearRemoteTransactionMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XACollectTransactionsMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XAFinalizeTransactionMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XATransactionCommitMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XATransactionCreateMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XATransactionPrepareMessageTask;
import com.hazelcast.client.impl.protocol.task.transaction.XATransactionRollbackMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListAddMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionallist.TransactionalListSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapDeleteMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetForUpdateMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapGetMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapIsEmptyMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutIfAbsentMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveIfSameMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceIfSameMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSetMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmap.TransactionalMapValuesWithPredicateMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapGetMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveEntryMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapValueCountMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueOfferMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePeekMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueuePollMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueSizeMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalqueue.TransactionalQueueTakeMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetAddMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetRemoveMessageTask;
import com.hazelcast.client.impl.protocol.task.transactionalset.TransactionalSetSizeMessageTask;
import com.hazelcast.cp.internal.client.AddCPGroupAvailabilityListenerMessageTask;
import com.hazelcast.cp.internal.client.AddCPMembershipListenerMessageTask;
import com.hazelcast.cp.internal.client.RemoveCPGroupAvailabilityListenerMessageTask;
import com.hazelcast.cp.internal.client.RemoveCPMembershipListenerMessageTask;
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
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.client.SqlCloseMessageTask;
import com.hazelcast.sql.impl.client.SqlExecuteMessageTask;
import com.hazelcast.sql.impl.client.SqlFetchMessageTask;
import com.hazelcast.sql.impl.client.SqlMappingDdlTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.util.MapUtil.createInt2ObjectHashMap;

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
        initializeSetTaskFactories();
        initializeRingBufferTaskFactories();
        initializeCacheTaskFactories();
        initializeReplicatedMapTaskFactories();
        initializeLongRegisterClientTaskFactories();
        initializeTransactionalListTaskFactories();
        initializeTransactionalMultiMapTaskFactories();
        initializeListTaskFactories();
        initializeTransactionalQueueTaskFactories();
        initializeMultiMapTaskFactories();
        initializeTopicTaskFactories();
        initializeTransactionalMapTaskFactories();
        initializeExecutorServiceTaskFactories();
        initializeDurableExecutorTaskFactories();
        initializeTransactionTaskFactories();
        initializeTransactionalSetTaskFactories();
        initializeMapTaskFactories();
        initializeGeneralTaskFactories();
        initializeQueueTaskFactories();
        initializeCardinalityTaskFactories();
        initializeScheduledExecutorTaskFactories();
        initializeContinuousMapQueryOperations();
        initializeDynamicConfigTaskFactories();
        initializeFlakeIdGeneratorTaskFactories();
        initializePnCounterTaskFactories();
        initializeCPGroupTaskFactories();
        initializeCPListenerTaskFactories();
        initializeAtomicLongTaskFactories();
        initializeAtomicReferenceTaskFactories();
        initializeCountDownLatchTaskFactories();
        initializeFencedLockTaskFactories();
        initializeSemaphoreTaskFactories();
        initializeManagementCenterTaskFactories();
        initializeSqlTaskFactories();
        initializeSchemaFactories();
    }

    private void initializeSetTaskFactories() {
        factories.put(SetRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetRemoveListenerMessageTask(cm, node, con));
        factories.put(SetClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetClearMessageTask(cm, node, con));
        factories.put(SetCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(SetContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetContainsAllMessageTask(cm, node, con));
        factories.put(SetIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetIsEmptyMessageTask(cm, node, con));
        factories.put(SetAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddAllMessageTask(cm, node, con));
        factories.put(SetAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddMessageTask(cm, node, con));
        factories.put(SetCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetCompareAndRetainAllMessageTask(cm, node, con));
        factories.put(SetGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetGetAllMessageTask(cm, node, con));
        factories.put(SetRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetRemoveMessageTask(cm, node, con));
        factories.put(SetAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddListenerMessageTask(cm, node, con));
        factories.put(SetContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetContainsMessageTask(cm, node, con));
        factories.put(SetSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetSizeMessageTask(cm, node, con));
    }

    private void initializeRingBufferTaskFactories() {
        factories.put(RingbufferReadOneCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferReadOneMessageTask(cm, node, con));
        factories.put(RingbufferAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferAddAllMessageTask(cm, node, con));
        factories.put(RingbufferCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferCapacityMessageTask(cm, node, con));
        factories.put(RingbufferTailSequenceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferTailSequenceMessageTask(cm, node, con));
        factories.put(RingbufferAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferAddMessageTask(cm, node, con));
        factories.put(RingbufferRemainingCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferRemainingCapacityMessageTask(cm, node, con));
        factories.put(RingbufferReadManyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferReadManyMessageTask(cm, node, con));
        factories.put(RingbufferHeadSequenceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferHeadSequenceMessageTask(cm, node, con));
        factories.put(RingbufferSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferSizeMessageTask(cm, node, con));
    }

    private void initializeCacheTaskFactories() {
        factories.put(CacheClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheClearMessageTask(cm, node, con));
        factories.put(CacheFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheFetchNearCacheInvalidationMetadataTask(cm, node, con));
        factories.put(CacheReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheReplaceMessageTask(cm, node, con));
        factories.put(CacheContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheContainsKeyMessageTask(cm, node, con));
        factories.put(CacheCreateConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheCreateConfigMessageTask(cm, node, con));
        factories.put(CacheGetAndReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAndReplaceMessageTask(cm, node, con));
        factories.put(CacheGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAllMessageTask(cm, node, con));
        factories.put(CachePutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutMessageTask(cm, node, con));
        factories.put(CacheAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddNearCacheInvalidationListenerTask(cm, node, con));
        factories.put(CachePutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutAllMessageTask(cm, node, con));
        factories.put(CacheSetExpiryPolicyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheSetExpiryPolicyMessageTask(cm, node, con));
        factories.put(CacheLoadAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheLoadAllMessageTask(cm, node, con));
        factories.put(CacheListenerRegistrationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheListenerRegistrationMessageTask(cm, node, con));
        factories.put(CacheAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddEntryListenerMessageTask(cm, node, con));
        factories.put(CacheRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(CacheRemoveInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveInvalidationListenerMessageTask(cm, node, con));
        factories.put(CacheDestroyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheDestroyMessageTask(cm, node, con));
        factories.put(CacheRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveMessageTask(cm, node, con));
        factories.put(CacheEntryProcessorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEntryProcessorMessageTask(cm, node, con));
        factories.put(CacheGetAndRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAndRemoveMessageTask(cm, node, con));
        factories.put(CacheManagementConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheManagementConfigMessageTask(cm, node, con));
        factories.put(CachePutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutIfAbsentMessageTask(cm, node, con));
        factories.put(CacheRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveAllMessageTask(cm, node, con));
        factories.put(CacheRemoveAllKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveAllKeysMessageTask(cm, node, con));
        factories.put(CacheIterateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheIterateMessageTask(cm, node, con));
        factories.put(CacheAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(CacheGetConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetConfigMessageTask(cm, node, con));
        factories.put(CacheGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetMessageTask(cm, node, con));
        factories.put(CacheRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(CacheSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheSizeMessageTask(cm, node, con));
        factories.put(CacheIterateEntriesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheIterateEntriesMessageTask(cm, node, con));
        factories.put(CacheEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEventJournalSubscribeTask(cm, node, con));
        factories.put(CacheEventJournalReadCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEventJournalReadTask<>(cm, node, con));
    }

    private void initializeReplicatedMapTaskFactories() {
        factories.put(ReplicatedMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapSizeMessageTask(cm, node, con));
        factories.put(ReplicatedMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(cm, node, con));
        factories.put(ReplicatedMapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapIsEmptyMessageTask(cm, node, con));
        factories.put(ReplicatedMapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapPutAllMessageTask(cm, node, con));
        factories.put(ReplicatedMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapContainsKeyMessageTask(cm, node, con));
        factories.put(ReplicatedMapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapContainsValueMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddNearCacheEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddNearCacheListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapGetMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerWithPredicateMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(ReplicatedMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapRemoveMessageTask(cm, node, con));
        factories.put(ReplicatedMapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapClearMessageTask(cm, node, con));
        factories.put(ReplicatedMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapValuesMessageTask(cm, node, con));
        factories.put(ReplicatedMapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapEntrySetMessageTask(cm, node, con));
        factories.put(ReplicatedMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapPutMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapKeySetMessageTask(cm, node, con));
    }

    private void initializeLongRegisterClientTaskFactories() {
        factories.put(LongRegisterDecrementAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterDecrementAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndAddMessageTask(cm, node, con));
        factories.put(LongRegisterAddAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterAddAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetMessageTask(cm, node, con));
        factories.put(LongRegisterSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterSetMessageTask(cm, node, con));
        factories.put(LongRegisterIncrementAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterIncrementAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndSetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndIncrementCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndIncrementMessageTask(cm, node, con));
    }

    private void initializeTransactionalListTaskFactories() {
        factories.put(TransactionalListSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListSizeMessageTask(cm, node, con));
        factories.put(TransactionalListRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListRemoveMessageTask(cm, node, con));
        factories.put(TransactionalListAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListAddMessageTask(cm, node, con));
    }

    private void initializeTransactionalMultiMapTaskFactories() {
        factories.put(TransactionalMultiMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapPutMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapRemoveEntryMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapGetMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapRemoveMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapSizeMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapValueCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapValueCountMessageTask(cm, node, con));
    }

    private void initializeListTaskFactories() {
        factories.put(ListGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListGetAllMessageTask(cm, node, con));
        factories.put(ListListIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListListIteratorMessageTask(cm, node, con));
        factories.put(ListSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSetMessageTask(cm, node, con));
        factories.put(ListAddAllWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddAllWithIndexMessageTask(cm, node, con));
        factories.put(ListCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(ListGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListGetMessageTask(cm, node, con));
        factories.put(ListRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveListenerMessageTask(cm, node, con));
        factories.put(ListRemoveWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveWithIndexMessageTask(cm, node, con));
        factories.put(ListAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddListenerMessageTask(cm, node, con));
        factories.put(ListIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIteratorMessageTask(cm, node, con));
        factories.put(ListClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListClearMessageTask(cm, node, con));
        factories.put(ListAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddAllMessageTask(cm, node, con));
        factories.put(ListAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddMessageTask(cm, node, con));
        factories.put(ListAddWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddWithIndexMessageTask(cm, node, con));
        factories.put(ListLastIndexOfCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListLastIndexOfMessageTask(cm, node, con));
        factories.put(ListRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveMessageTask(cm, node, con));
        factories.put(ListSubCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSubMessageTask(cm, node, con));
        factories.put(ListContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListContainsMessageTask(cm, node, con));
        factories.put(ListIndexOfCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIndexOfMessageTask(cm, node, con));
        factories.put(ListSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSizeMessageTask(cm, node, con));
        factories.put(ListContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListContainsAllMessageTask(cm, node, con));
        factories.put(ListIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIsEmptyMessageTask(cm, node, con));
        factories.put(ListCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListCompareAndRetainAllMessageTask(cm, node, con));
    }

    private void initializeTransactionalQueueTaskFactories() {
        factories.put(TransactionalQueueSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueSizeMessageTask(cm, node, con));
        factories.put(TransactionalQueueOfferCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueOfferMessageTask(cm, node, con));
        factories.put(TransactionalQueuePeekCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueuePeekMessageTask(cm, node, con));
        factories.put(TransactionalQueuePollCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueuePollMessageTask(cm, node, con));
        factories.put(TransactionalQueueTakeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueTakeMessageTask(cm, node, con));
    }

    private void initializeMultiMapTaskFactories() {
        factories.put(MultiMapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapClearMessageTask(cm, node, con));
        factories.put(MultiMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapGetMessageTask(cm, node, con));
        factories.put(MultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveEntryMessageTask(cm, node, con));
        factories.put(MultiMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsKeyMessageTask(cm, node, con));
        factories.put(MultiMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapSizeMessageTask(cm, node, con));
        factories.put(MultiMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(MultiMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapAddEntryListenerMessageTask(cm, node, con));
        factories.put(MultiMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveMessageTask(cm, node, con));
        factories.put(MultiMapTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapTryLockMessageTask(cm, node, con));
        factories.put(MultiMapIsLockedCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapIsLockedMessageTask(cm, node, con));
        factories.put(MultiMapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsValueMessageTask(cm, node, con));
        factories.put(MultiMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapKeySetMessageTask(cm, node, con));
        factories.put(MultiMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapPutMessageTask(cm, node, con));
        factories.put(MultiMapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapPutAllMessageTask(cm, node, con));
        factories.put(MultiMapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapEntrySetMessageTask(cm, node, con));
        factories.put(MultiMapValueCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapValueCountMessageTask(cm, node, con));
        factories.put(MultiMapUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapUnlockMessageTask(cm, node, con));
        factories.put(MultiMapLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapLockMessageTask(cm, node, con));
        factories.put(MultiMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(MultiMapContainsEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsEntryMessageTask(cm, node, con));
        factories.put(MultiMapForceUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapForceUnlockMessageTask(cm, node, con));
        factories.put(MultiMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapValuesMessageTask(cm, node, con));
        factories.put(MultiMapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapDeleteMessageTask(cm, node, con));
    }

    private void initializeTopicTaskFactories() {
        factories.put(TopicPublishCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicPublishMessageTask(cm, node, con));
        factories.put(TopicPublishAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicPublishAllMessageTask(cm, node, con));
        factories.put(TopicAddMessageListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicAddMessageListenerMessageTask(cm, node, con));
        factories.put(TopicRemoveMessageListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicRemoveMessageListenerMessageTask(cm, node, con));
    }

    private void initializeTransactionalMapTaskFactories() {
        factories.put(TransactionalMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapValuesMessageTask(cm, node, con));
        factories.put(TransactionalMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapSizeMessageTask(cm, node, con));
        factories.put(TransactionalMapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapPutIfAbsentMessageTask(cm, node, con));
        factories.put(TransactionalMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapRemoveMessageTask(cm, node, con));
        factories.put(TransactionalMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapGetMessageTask(cm, node, con));
        factories.put(TransactionalMapGetForUpdateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapGetForUpdateMessageTask(cm, node, con));
        factories.put(TransactionalMapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapIsEmptyMessageTask(cm, node, con));
        factories.put(TransactionalMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapKeySetMessageTask(cm, node, con));
        factories.put(TransactionalMapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapKeySetWithPredicateMessageTask(cm, node, con));
        factories.put(TransactionalMapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapReplaceIfSameMessageTask(cm, node, con));
        factories.put(TransactionalMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapContainsKeyMessageTask(cm, node, con));
        factories.put(TransactionalMapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapRemoveIfSameMessageTask(cm, node, con));
        factories.put(TransactionalMapSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapSetMessageTask(cm, node, con));
        factories.put(TransactionalMapReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapReplaceMessageTask(cm, node, con));
        factories.put(TransactionalMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapPutMessageTask(cm, node, con));
        factories.put(TransactionalMapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapDeleteMessageTask(cm, node, con));
        factories.put(TransactionalMapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapValuesWithPredicateMessageTask(cm, node, con));
    }

    private void initializeExecutorServiceTaskFactories() {
        factories.put(ExecutorServiceCancelOnPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceCancelOnPartitionMessageTask(cm, node, con));
        factories.put(ExecutorServiceSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceSubmitToPartitionMessageTask(cm, node, con));
        factories.put(ExecutorServiceCancelOnMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceCancelOnAddressMessageTask(cm, node, con));
        factories.put(ExecutorServiceIsShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceIsShutdownMessageTask(cm, node, con));
        factories.put(ExecutorServiceShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceShutdownMessageTask(cm, node, con));
        factories.put(ExecutorServiceSubmitToMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceSubmitToAddressMessageTask(cm, node, con));
    }

    private void initializeDurableExecutorTaskFactories() {
        factories.put(DurableExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorSubmitToPartitionMessageTask(cm, node, con));
        factories.put(DurableExecutorIsShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorIsShutdownMessageTask(cm, node, con));
        factories.put(DurableExecutorShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorShutdownMessageTask(cm, node, con));
        factories.put(DurableExecutorRetrieveResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorRetrieveResultMessageTask(cm, node, con));
        factories.put(DurableExecutorDisposeResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorDisposeResultMessageTask(cm, node, con));
        factories.put(DurableExecutorRetrieveAndDisposeResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorRetrieveAndDisposeResultMessageTask(cm, node, con));
    }

    private void initializeTransactionTaskFactories() {
        factories.put(TransactionCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionCreateMessageTask(cm, node, con));
        factories.put(XATransactionClearRemoteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XAClearRemoteTransactionMessageTask(cm, node, con));
        factories.put(XATransactionFinalizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XAFinalizeTransactionMessageTask(cm, node, con));
        factories.put(TransactionCommitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionCommitMessageTask(cm, node, con));
        factories.put(XATransactionCollectTransactionsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XACollectTransactionsMessageTask(cm, node, con));
        factories.put(XATransactionPrepareCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionPrepareMessageTask(cm, node, con));
        factories.put(XATransactionCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionCreateMessageTask(cm, node, con));
        factories.put(TransactionRollbackCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionRollbackMessageTask(cm, node, con));
        factories.put(XATransactionCommitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionCommitMessageTask(cm, node, con));
        factories.put(XATransactionRollbackCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionRollbackMessageTask(cm, node, con));
    }

    private void initializeTransactionalSetTaskFactories() {
        factories.put(TransactionalSetSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetSizeMessageTask(cm, node, con));
        factories.put(TransactionalSetAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetAddMessageTask(cm, node, con));
        factories.put(TransactionalSetRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetRemoveMessageTask(cm, node, con));
    }

    private void initializeMapTaskFactories() {
        factories.put(MapEntriesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntriesWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerMessageTask(cm, node, con));
        factories.put(MapFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchNearCacheInvalidationMetadataTask(cm, node, con));
        factories.put(MapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveIfSameMessageTask(cm, node, con));
        factories.put(MapAddInterceptorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddInterceptorMessageTask(cm, node, con));
        factories.put(MapEntriesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntriesWithPredicateMessageTask(cm, node, con));
        factories.put(MapPutTransientCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutTransientMessageTask(cm, node, con));
        factories.put(MapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapContainsValueMessageTask(cm, node, con));
        factories.put(MapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapIsEmptyMessageTask(cm, node, con));
        factories.put(MapReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceMessageTask(cm, node, con));
        factories.put(MapRemoveInterceptorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveInterceptorMessageTask(cm, node, con));
        factories.put(MapAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddNearCacheInvalidationListenerMessageTask(cm, node, con));
        factories.put(MapExecuteOnAllKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnAllKeysMessageTask(cm, node, con));
        factories.put(MapFlushCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFlushMessageTask(cm, node, con));
        factories.put(MapSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetMessageTask(cm, node, con));
        factories.put(MapTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryLockMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(MapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntrySetMessageTask(cm, node, con));
        factories.put(MapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapClearMessageTask(cm, node, con));
        factories.put(MapLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLockMessageTask(cm, node, con));
        factories.put(MapGetEntryViewCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetEntryViewMessageTask(cm, node, con));
        factories.put(MapRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(MapLoadGivenKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLoadGivenKeysMessageTask(cm, node, con));
        factories.put(MapExecuteWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteWithPredicateMessageTask(cm, node, con));
        factories.put(MapRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveAllMessageTask(cm, node, con));
        factories.put(MapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutIfAbsentMessageTask(cm, node, con));
        factories.put(MapTryRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryRemoveMessageTask(cm, node, con));
        factories.put(MapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutMessageTask(cm, node, con));
        factories.put(MapUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapUnlockMessageTask(cm, node, con));
        factories.put(MapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSizeMessageTask(cm, node, con));
        factories.put(MapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesWithPredicateMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerToKeyWithPredicateMessageTask(cm, node, con));
        factories.put(MapEvictCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEvictMessageTask(cm, node, con));
        factories.put(MapGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetAllMessageTask(cm, node, con));
        factories.put(MapReplaceAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceAllMessageTask(cm, node, con));
        factories.put(MapForceUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapForceUnlockMessageTask(cm, node, con));
        factories.put(MapLoadAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLoadAllMessageTask(cm, node, con));
        factories.put(MapAddIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddIndexMessageTask(cm, node, con));
        factories.put(MapExecuteOnKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnKeyMessageTask(cm, node, con));
        factories.put(MapKeySetWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(MapIsLockedCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapIsLockedMessageTask(cm, node, con));
        factories.put(MapEvictAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEvictAllMessageTask(cm, node, con));
        factories.put(MapSubmitToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSubmitToKeyMessageTask(cm, node, con));
        factories.put(MapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerWithPredicateMessageTask(cm, node, con));
        factories.put(MapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapDeleteMessageTask(cm, node, con));
        factories.put(MapAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(MapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutAllMessageTask(cm, node, con));
        factories.put(MapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveMessageTask(cm, node, con));
        factories.put(MapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetWithPredicateMessageTask(cm, node, con));
        factories.put(MapExecuteOnKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnKeysMessageTask(cm, node, con));
        factories.put(MapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceIfSameMessageTask(cm, node, con));
        factories.put(MapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapContainsKeyMessageTask(cm, node, con));
        factories.put(MapTryPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryPutMessageTask(cm, node, con));
        factories.put(MapValuesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetMessageTask(cm, node, con));
        factories.put(MapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetMessageTask(cm, node, con));
        factories.put(MapFetchKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchKeysMessageTask(cm, node, con));
        factories.put(MapFetchEntriesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchEntriesMessageTask(cm, node, con));
        factories.put(MapAggregateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAggregateMessageTask(cm, node, con));
        factories.put(MapAggregateWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAggregateWithPredicateMessageTask(cm, node, con));
        factories.put(MapProjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapProjectionMessageTask(cm, node, con));
        factories.put(MapProjectWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapProjectionWithPredicateMessageTask(cm, node, con));
        factories.put(MapFetchWithQueryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchWithQueryMessageTask(cm, node, con));
        factories.put(MapEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEventJournalSubscribeTask(cm, node, con));
        factories.put(MapEventJournalReadCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEventJournalReadTask<>(cm, node, con));
        factories.put(MapSetTtlCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetTtlMessageTask(cm, node, con));
        factories.put(MapSetWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutIfAbsentWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutIfAbsentWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutTransientWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutTransientWithMaxIdleMessageTask(cm, node, con));
    }

    private void initializeGeneralTaskFactories() {
        factories.put(ClientAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(ClientRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(ClientAddMigrationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMigrationListenerMessageTask(cm, node, con));
        factories.put(ClientRemoveMigrationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveMigrationListenerMessageTask(cm, node, con));
        factories.put(ClientCreateProxyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateProxyMessageTask(cm, node, con));
        factories.put(ClientGetDistributedObjectsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetDistributedObjectsMessageTask(cm, node, con));
        factories.put(ClientAddDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddDistributedObjectListenerMessageTask(cm, node, con));
        factories.put(ClientDestroyProxyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DestroyProxyMessageTask(cm, node, con));
        factories.put(ClientPingCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PingMessageTask(cm, node, con));
        factories.put(ClientAddClusterViewListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddClusterViewListenerMessageTask(cm, node, con));
        factories.put(ClientAuthenticationCustomCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AuthenticationCustomCredentialsMessageTask(cm, node, con));
        factories.put(ClientRemoveDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveDistributedObjectListenerMessageTask(cm, node, con));
        factories.put(ClientAuthenticationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AuthenticationMessageTask(cm, node, con));
        factories.put(ClientStatisticsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ClientStatisticsMessageTask(cm, node, con));
        factories.put(ClientDeployClassesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DeployClassesMessageTask(cm, node, con));
        factories.put(ClientCreateProxiesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateProxiesMessageTask(cm, node, con));
        factories.put(ClientLocalBackupListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddBackupListenerMessageTask(cm, node, con));
        factories.put(ClientTriggerPartitionAssignmentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TriggerPartitionAssignmentMessageTask(cm, node, con));
    }

    private void initializeQueueTaskFactories() {
        factories.put(QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(QueueContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueContainsAllMessageTask(cm, node, con));
        factories.put(QueueAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueAddAllMessageTask(cm, node, con));
        factories.put(QueueTakeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueTakeMessageTask(cm, node, con));
        factories.put(QueueAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueAddListenerMessageTask(cm, node, con));
        factories.put(QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueCompareAndRetainAllMessageTask(cm, node, con));
        factories.put(QueueOfferCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueOfferMessageTask(cm, node, con));
        factories.put(QueuePeekCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePeekMessageTask(cm, node, con));
        factories.put(QueueRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemoveMessageTask(cm, node, con));
        factories.put(QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueIsEmptyMessageTask(cm, node, con));
        factories.put(QueueIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueIteratorMessageTask(cm, node, con));
        factories.put(QueueSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueSizeMessageTask(cm, node, con));
        factories.put(QueuePutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePutMessageTask(cm, node, con));
        factories.put(QueueContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueContainsMessageTask(cm, node, con));
        factories.put(QueuePollCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePollMessageTask(cm, node, con));
        factories.put(QueueDrainToCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueDrainMessageTask(cm, node, con));
        factories.put(QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemoveListenerMessageTask(cm, node, con));
        factories.put(QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemainingCapacityMessageTask(cm, node, con));
        factories.put(QueueClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueClearMessageTask(cm, node, con));
        factories.put(QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueDrainMaxSizeMessageTask(cm, node, con));
    }

    private void initializeCardinalityTaskFactories() {
        factories.put(CardinalityEstimatorAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CardinalityEstimatorAddMessageTask(cm, node, con));
        factories.put(CardinalityEstimatorEstimateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CardinalityEstimatorEstimateMessageTask(cm, node, con));
    }

    private void initializeScheduledExecutorTaskFactories() {
        factories.put(ScheduledExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorSubmitToPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorSubmitToMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorSubmitToTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorShutdownMessageTask(cm, node, con));
        factories.put(ScheduledExecutorDisposeFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskDisposeFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorDisposeFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskDisposeFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorCancelFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskCancelFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorCancelFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskCancelFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsDoneFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsDoneFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsDoneFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsDoneFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetDelayFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetDelayFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetDelayFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetDelayFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetStatsFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetStatsFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetStatisticsFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetResultFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetResultFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetResultFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetResultFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetAllScheduledFuturesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorGetAllScheduledMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsCancelledFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsCancelledFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsCancelledFromTargetMessageTask(cm, node, con));
    }

    private void initializeContinuousMapQueryOperations() {
        factories.put(ContinuousQueryDestroyCacheCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapDestroyCacheMessageTask(cm, node, con));
        factories.put(ContinuousQueryPublisherCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPublisherCreateMessageTask(cm, node, con));
        factories.put(ContinuousQuerySetReadCursorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetReadCursorMessageTask(cm, node, con));
        factories.put(ContinuousQueryAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddListenerMessageTask(cm, node, con));
        factories.put(ContinuousQueryMadePublishableCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapMadePublishableMessageTask(cm, node, con));
        factories.put(ContinuousQueryPublisherCreateWithValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPublisherCreateWithValueMessageTask(cm, node, con));
    }

    private void initializeDynamicConfigTaskFactories() {
        factories.put(DynamicConfigAddMultiMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMultiMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddCardinalityEstimatorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCardinalityEstimatorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddDurableExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddDurableExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddScheduledExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddScheduledExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddRingbufferConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddRingbufferConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddListConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddListConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddSetConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddSetConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddTopicConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddTopicConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddReplicatedMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddReplicatedMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddQueueConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddQueueConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddReliableTopicConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddReliableTopicConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddCacheConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCacheConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddFlakeIdGeneratorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddFlakeIdGeneratorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddPNCounterConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddPNCounterConfigMessageTask(cm, node, con));
    }

    private void initializeFlakeIdGeneratorTaskFactories() {
        factories.put(FlakeIdGeneratorNewIdBatchCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new NewIdBatchMessageTask(cm, node, con));
    }

    private void initializePnCounterTaskFactories() {
        factories.put(PNCounterGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterGetMessageTask(cm, node, con));
        factories.put(PNCounterAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterAddMessageTask(cm, node, con));
        factories.put(PNCounterGetConfiguredReplicaCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterGetConfiguredReplicaCountMessageTask(cm, node, con));
    }

    private void initializeCPGroupTaskFactories() {
        factories.put(CPGroupCreateCPGroupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateRaftGroupMessageTask(cm, node, con));
        factories.put(CPGroupDestroyCPObjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DestroyRaftObjectMessageTask(cm, node, con));

        factories.put(CPSessionCreateSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateSessionMessageTask(cm, node, con));
        factories.put(CPSessionHeartbeatSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HeartbeatSessionMessageTask(cm, node, con));
        factories.put(CPSessionCloseSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CloseSessionMessageTask(cm, node, con));
        factories.put(CPSessionGenerateThreadIdCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GenerateThreadIdMessageTask(cm, node, con));
    }

    private void initializeCPListenerTaskFactories() {
        factories.put(CPSubsystemAddMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCPMembershipListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemRemoveMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveCPMembershipListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemAddGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCPGroupAvailabilityListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemRemoveGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveCPGroupAvailabilityListenerMessageTask(cm, node, con));
    }

    private void initializeAtomicLongTaskFactories() {
        factories.put(AtomicLongAddAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddAndGetMessageTask(cm, node, con));
        factories.put(AtomicLongCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CompareAndSetMessageTask(cm, node, con));
        factories.put(AtomicLongGetAndAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetAndAddMessageTask(cm, node, con));
        factories.put(AtomicLongGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMessageTask(cm, node, con));
        factories.put(AtomicLongGetAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetAndSetMessageTask(cm, node, con));
        factories.put(AtomicLongApplyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ApplyMessageTask(cm, node, con));
        factories.put(AtomicLongAlterCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AlterMessageTask(cm, node, con));
    }

    private void initializeAtomicReferenceTaskFactories() {
        factories.put(AtomicRefApplyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client.ApplyMessageTask(cm, node, con));
        factories.put(AtomicRefSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetMessageTask(cm, node, con));
        factories.put(AtomicRefContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ContainsMessageTask(cm, node, con));
        factories.put(AtomicRefGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client.GetMessageTask(cm, node, con));
        factories.put(AtomicRefCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client.CompareAndSetMessageTask(cm, node, con));
    }

    private void initializeCountDownLatchTaskFactories() {
        factories.put(CountDownLatchAwaitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AwaitMessageTask(cm, node, con));
        factories.put(CountDownLatchCountDownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CountDownMessageTask(cm, node, con));
        factories.put(CountDownLatchGetCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetCountMessageTask(cm, node, con));
        factories.put(CountDownLatchGetRoundCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetRoundMessageTask(cm, node, con));
        factories.put(CountDownLatchTrySetCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TrySetCountMessageTask(cm, node, con));
    }

    private void initializeFencedLockTaskFactories() {
        factories.put(FencedLockLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LockMessageTask(cm, node, con));
        factories.put(FencedLockTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TryLockMessageTask(cm, node, con));
        factories.put(FencedLockUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UnlockMessageTask(cm, node, con));
        factories.put(FencedLockGetLockOwnershipCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetLockOwnershipStateMessageTask(cm, node, con));
    }

    private void initializeSemaphoreTaskFactories() {
        factories.put(SemaphoreAcquireCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AcquirePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreAvailablePermitsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AvailablePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreChangeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreDrainCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DrainPermitsMessageTask(cm, node, con));
        factories.put(SemaphoreGetSemaphoreTypeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetSemaphoreTypeMessageTask(cm, node, con));
        factories.put(SemaphoreInitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new InitSemaphoreMessageTask(cm, node, con));
        factories.put(SemaphoreReleaseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReleasePermitsMessageTask(cm, node, con));
    }

    private void initializeManagementCenterTaskFactories() {
        factories.put(MCReadMetricsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReadMetricsMessageTask(cm, node, con));
        factories.put(MCChangeClusterStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeClusterStateMessageTask(cm, node, con));
        factories.put(MCGetMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMapConfigMessageTask(cm, node, con));
        factories.put(MCUpdateMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UpdateMapConfigMessageTask(cm, node, con));
        factories.put(MCGetMemberConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMemberConfigMessageTask(cm, node, con));
        factories.put(MCRunGcCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunGcMessageTask(cm, node, con));
        factories.put(MCGetThreadDumpCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetThreadDumpMessageTask(cm, node, con));
        factories.put(MCShutdownMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ShutdownMemberMessageTask(cm, node, con));
        factories.put(MCPromoteLiteMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PromoteLiteMemberMessageTask(cm, node, con));
        factories.put(MCGetSystemPropertiesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetSystemPropertiesMessageTask(cm, node, con));
        factories.put(MCGetTimedMemberStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetTimedMemberStateMessageTask(cm, node, con));
        factories.put(MCMatchMCConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MatchClientFilteringConfigMessageTask(cm, node, con));
        factories.put(MCApplyMCConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ApplyClientFilteringConfigMessageTask(cm, node, con));
        factories.put(MCGetClusterMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetClusterMetadataMessageTask(cm, node, con));
        factories.put(MCShutdownClusterCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ShutdownClusterMessageTask(cm, node, con));
        factories.put(MCChangeClusterVersionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeClusterVersionMessageTask(cm, node, con));
        factories.put(MCRunScriptCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunScriptMessageTask(cm, node, con));
        factories.put(MCRunConsoleCommandCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunConsoleCommandMessageTask(cm, node, con));
        factories.put(com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeWanReplicationStateMessageTask(cm, node, con));
        factories.put(com.hazelcast.client.impl.protocol.codec.MCClearWanQueuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ClearWanQueuesMessageTask(cm, node, con));
        factories.put(com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddWanBatchPublisherConfigMessageTask(cm, node, con));
        factories.put(com.hazelcast.client.impl.protocol.codec.MCWanSyncMapCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new WanSyncMapMessageTask(cm, node, con));
        factories.put(com.hazelcast.client.impl.protocol.codec.MCCheckWanConsistencyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CheckWanConsistencyMessageTask(cm, node, con));
        factories.put(MCPollMCEventsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PollMCEventsMessageTask(cm, node, con));
        factories.put(MCGetCPMembersCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetCPMembersMessageTask(cm, node, con));
        factories.put(MCPromoteToCPMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PromoteToCPMemberMessageTask(cm, node, con));
        factories.put(MCRemoveCPMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveCPMemberMessageTask(cm, node, con));
        factories.put(MCResetCPSubsystemCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ResetCPSubsystemMessageTask(cm, node, con));
        factories.put(MCTriggerPartialStartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerPartialStartMessageTask(cm, node, con));
        factories.put(MCTriggerForceStartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerForceStartMessageTask(cm, node, con));
        factories.put(MCTriggerHotRestartBackupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerBackupMessageTask(cm, node, con));
        factories.put(MCInterruptHotRestartBackupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartInterruptBackupMessageTask(cm, node, con));
        factories.put(MCResetQueueAgeStatisticsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueResetAgeStatisticsMessageTask(cm, node, con));
        factories.put(MCReloadConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReloadConfigMessageTask(cm, node, con));
        factories.put(MCUpdateConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UpdateConfigMessageTask(cm, node, con));
    }

    private void initializeSqlTaskFactories() {
        factories.put(SqlExecuteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlExecuteMessageTask(cm, node, con));
        factories.put(SqlFetchCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlFetchMessageTask(cm, node, con));
        factories.put(SqlCloseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlCloseMessageTask(cm, node, con));
        factories.put(SqlMappingDdlCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlMappingDdlTask(cm, node, con));
    }

    private void initializeSchemaFactories() {
        factories.put(ClientSendSchemaCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SendSchemaMessageTask(cm, node, con));
        factories.put(ClientFetchSchemaCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new FetchSchemaMessageTask(cm, node, con));
        factories.put(ClientSendAllSchemasCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SendAllSchemasMessageTask(cm, node, con));
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
