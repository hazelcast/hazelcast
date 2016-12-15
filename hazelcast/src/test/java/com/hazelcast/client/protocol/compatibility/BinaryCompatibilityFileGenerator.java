package com.hazelcast.client.protocol.compatibility;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceClearCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceIsNullCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAssignAndGetUuidsCodec;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
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
import com.hazelcast.client.impl.protocol.codec.CacheSizeCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ConditionAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.ConditionBeforeAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.ConditionSignalAllCodec;
import com.hazelcast.client.impl.protocol.codec.ConditionSignalCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapMadePublishableCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateWithValueCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapSetReadCursorCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
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
import com.hazelcast.client.impl.protocol.codec.LockForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetLockCountCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetRemainingLeaseTimeCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedByCurrentThreadCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.LockLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAssignAndGetUuidsCodec;
import com.hazelcast.client.impl.protocol.codec.MapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MapClearNearCacheCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec;
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
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceCancelCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForCustomCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForListCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForMapCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForMultiMapCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForSetCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceJobProcessInformationCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetCodec;
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
import com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec;
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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainPermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReducePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreTryAcquireCodec;
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
import com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec;
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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aBoolean;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aByte;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aData;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aListOfEntry;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aLong;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aMember;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aPartitionTable;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aQueryCacheEventData;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aString;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aUUID;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anAddress;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anEntryView;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anInt;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anXid;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.cacheEventDatas;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.datas;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.distributedObjectInfos;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.jobPartitionStates;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.longs;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.members;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.queryCacheEventDatas;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.strings;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.uuids;


public class BinaryCompatibilityFileGenerator {
    public static void main(String[] args) throws IOException {
        OutputStream out = new FileOutputStream("1.4.protocol.compatibility.binary");
        DataOutputStream outputStream = new DataOutputStream(out);

{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeRequest(    aString ,    aString ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeRequest(    aData ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberEvent( aMember ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent( members   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberAttributeChangeEvent( aString ,  aString ,  anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeResponse(    aPartitionTable   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodePartitionLostEvent( anInt ,  anInt ,  anAddress   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeResponse(    distributedObjectInfos   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeDistributedObjectEvent( aString ,  aString ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientPingCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientPingCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapDeleteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFlushCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFlushCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutTransientCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutTransientCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSetCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapIsLockedCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent( aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent( datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeMapPartitionLostEvent( anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeResponse(    anEntryView   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEvictCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEvictCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEvictAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEvictAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLoadAllCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLoadAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetAllCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddIndexCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddIndexCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeRequest(    aString ,    aData ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeRequest(    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeResponse(    anInt ,    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAggregateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAggregateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapProjectCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapProjectCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapGetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueOfferCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueOfferCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePutCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePutCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePollCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePollCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueTakeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueTakeCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePeekCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePeekCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueIteratorCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueDrainToCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueDrainToCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = QueueAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TopicPublishCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicPublishCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeTopicEvent( aData ,  aLong ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListGetAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListGetAllCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ListAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeRequest(    aString ,    anInt ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListGetCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSetCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIndexOfCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIndexOfCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSubCodec.encodeRequest(    aString ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSubCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIteratorCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListListIteratorCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListListIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetGetAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetGetAllCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = SetAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockIsLockedCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockUnlockCodec.encodeRequest(    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockTryLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeRequest(    aString ,    aLong ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionSignalCodec.encodeRequest(    aString ,    aLong ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionSignalCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeRequest(    aString ,    aLong ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeRequest(    aString ,    anAddress ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeRequest(    aString ,    aString ,    aData ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeRequest(    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeRequest(    aString ,    anInt ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeResponse(    jobPartitionStates ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForListCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForListCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aData ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeCacheEvent( anInt ,  cacheEventDatas ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeRequest(    aString ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeRequest(    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheDestroyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheDestroyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeRequest(    aString ,    aData ,    aData ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAllCodec.encodeRequest(    aString ,    datas ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAllCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeRequest(    aString ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheIterateCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheIterateCodec.encodeResponse(    anInt ,    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aBoolean ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveCodec.encodeRequest(    aString ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent( anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutAllCodec.encodeRequest(    aString ,    aListOfEntry ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeRequest(    anXid   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeRequest(    anXid ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeRequest(    anXid ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionCommitCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionCommitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionCreateCodec.encodeRequest(    aLong ,    anInt ,    anInt ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionCreateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = EnterpriseMapPublisherCreateWithValueCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapPublisherCreateWithValueCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = EnterpriseMapPublisherCreateCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapPublisherCreateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = EnterpriseMapMadePublishableCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapMadePublishableCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeQueryCacheSingleEvent( aQueryCacheEventData   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeQueryCacheBatchEvent( queryCacheEventDatas ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = EnterpriseMapSetReadCursorCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapSetReadCursorCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = EnterpriseMapDestroyCacheCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = EnterpriseMapDestroyCacheCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferAddCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferAddCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeRequest(    aString ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeResponse(    anInt ,    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeRequest(    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeRequest(    aString ,    anAddress ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeResponse(    strings   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetStatsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsCodec.encodeResponse(    aLong ,    aLong ,    aLong ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetDelayCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorCancelCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsDoneCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetResultCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorDisposeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}

         outputStream.close();
         out.close();

    }
}

