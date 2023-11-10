/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.map.impl.MapService;

public final class SecurityInterceptorConstants {
    // Service Names
    public static final String ICACHE_SERVICE = ICacheService.SERVICE_NAME;
    public static final String IMAP_SERVICE = MapService.SERVICE_NAME;
    public static final String PN_COUNTER_SERVICE = PNCounterService.SERVICE_NAME;
    public static final String CARDINALITY_ESTIMATOR_SERVICE = CardinalityEstimatorService.SERVICE_NAME;

    // Method names
    public static final String ADD_ENTRY_LISTENER = "addEntryListener";
    public static final String ADD_NEAR_CACHE_INVALIDATION_LISTENER = "addNearCacheInvalidationListener";
    public static final String REMOVE_INVALIDATION_LISTENER = "removeInvalidationListener";
    public static final String ADD_PARTITION_LOST_LISTENER = "addPartitionLostListener";
    public static final String REMOVE_PARTITION_LOST_LISTENER = "removePartitionLostListener";
    public static final String CLEAR = "clear";
    public static final String CONTAINS_KEY = "containsKey";
    public static final String DESTROY = "destroy";
    public static final String CREATE_CONFIG = "createConfig";
    public static final String INVOKE = "invoke";
    public static final String READ_FROM_EVENT_JOURNAL = "readFromEventJournal";
    public static final String SUBSCRIBE_TO_EVENT_JOURNAL = "subscribeToEventJournal";
    public static final String GET_ALL = "getAll";
    public static final String GET_AND_REMOVE = "getAndRemove";
    public static final String GET_AND_REPLACE = "getAndReplace";
    public static final String GET_CONFIG = "getConfig";
    public static final String GET = "get";
    public static final String ITERATOR = "iterator";
    public static final String REGISTER_CACHE_ENTRY_LISTENER = "registerCacheEntryListener";
    public static final String DEREGISTER_CACHE_ENTRY_LISTENER = "deregisterCacheEntryListener";
    public static final String LOAD_ALL = "loadAll";
    public static final String ENABLE_MANAGEMENT = "enableManagement";
    public static final String PUT_ALL = "putAll";
    public static final String PUT_IF_ABSENT = "putIfAbsent";
    public static final String PUT = "put";
    public static final String GET_AND_PUT = "getAndPut";
    public static final String REMOVE = "remove";
    public static final String REMOVE_ALL = "removeAll";
    public static final String REPLACE = "replace";
    public static final String SET_EXPIRY_POLICY = "setExpiryPolicy";
    public static final String SIZE = "size";

    public static final String ADD = "add";
    public static final String ESTIMATE = "estimate";

    public static final String GET_CONFIGURED_REPLICA_COUNT = "getConfiguredReplicaCount";

    public static final String ADD_INDEX = "addIndex";
    public static final String ADD_INTERCEPTOR = "addInterceptor";
    public static final String AGGREGATE = "aggregate";
    public static final String AGGREGATE_WITH_PREDICATE = "aggregateWithPredicate";
    public static final String CONTAINS_VALUE = "containsValue";
    public static final String DELETE = "delete";
    public static final String DESTROY_CACHE = "destroyCache";
    public static final String ENTRY_SET = "entrySet";
    public static final String EVICT_ALL = "evictAll";
    public static final String EVICT = "evict";
    public static final String EXECUTE_ON_ENTRIES = "executeOnEntries";
    public static final String EXECUTE_ON_KEY = "executeOnKey";
    public static final String EXECUTE_ON_KEYS = "executeOnKeys";
    public static final String ITERATOR_FETCH_ENTRIES = "iteratorFetchEntries";
    public static final String ITERATOR_FETCH_KEYS = "iteratorFetchKeys";
    public static final String FETCH_NEAR_CACHE_INVALIDATION_METADATA = "fetchNearCacheInvalidationMetadata";
    public static final String ITERATOR_FETCH_WITH_QUERY = "iteratorFetchWithQuery";
    public static final String FLUSH = "flush";
    public static final String FORCE_UNLOCK = "forceUnlock";
    public static final String GET_ENTRY_VIEW = "getEntryView";
    public static final String IS_EMPTY = "isEmpty";
    public static final String IS_LOCKED = "isLocked";
    public static final String KEY_SET = "keySet";
    public static final String LOCK = "lock";
    public static final String PROJECT = "project";
    public static final String PUT_TRANSIENT = "putTransient";
    public static final String REMOVE_ENTRY_LISTENER = "removeEntryListener";
    public static final String REMOVE_INTERCEPTOR = "removeInterceptor";
    public static final String REPLACE_ALL = "replaceAll";
    public static final String SET = "set";
    public static final String SET_TTL = "setTtl";
    public static final String SUBMIT_TO_KEY = "submitToKey";
    public static final String TRY_LOCK = "tryLock";
    public static final String TRY_PUT = "tryPut";
    public static final String TRY_REMOVE = "tryRemove";
    public static final String UNLOCK = "unlock";
    public static final String VALUES = "values";
    public static final String ADD_CACHE_CONFIG = "addCacheConfig";
    public static final String ADD_CARDINALITY_ESTIMATOR_CONFIG = "addCardinalityEstimatorConfig";
    public static final String ADD_DATA_CONNECTION_CONFIG = "addDataConnectionConfig";
    public static final String ADD_DURABLE_EXECUTOR_CONFIG = "addDurableExecutorConfig";
    public static final String ADD_EXECUTOR_CONFIG = "addExecutorConfig";
    public static final String ADD_FLAKE_ID_GENERATOR_CONFIG = "addFlakeIdGeneratorConfig";
    public static final String ADD_LIST_CONFIG = "addListConfig";
    public static final String ADD_MAP_CONFIG = "addMapConfig";
    public static final String ADD_MULTIMAP_CONFIG = "addMultiMapConfig";
    public static final String ADD_PN_COUNTER_CONFIG = "addPNCounterConfig";
    public static final String ADD_QUEUE_CONFIG = "addQueueConfig";
    public static final String ADD_RELIABLE_TOPIC_CONFIG = "addReliableTopicConfig";
    public static final String ADD_REPLICATED_MAP_CONFIG = "addReplicatedMapConfig";
    public static final String ADD_RINGBUFFER_CONFIG = "addRingbufferConfig";
    public static final String ADD_SCHEDULED_EXECUTOR_CONFIG = "addScheduledExecutorConfig";
    public static final String ADD_SET_CONFIG = "addSetConfig";
    public static final String ADD_TOPIC_CONFIG = "addTopicConfig";
    public static final String DISPOSE_RESULT = "disposeResult";
    public static final String IS_SHUTDOWN = "isShutdown";
    public static final String RETRIEVE_AND_DISPOSE_RESULT = "retrieveAndDisposeResult";
    public static final String RETRIEVE_RESULT = "retrieveResult";
    public static final String SHUTDOWN = "shutdown";
    public static final String SUBMIT_TO_PARTITION = "submitToPartition";
    public static final String CANCEL = "cancel";
    public static final String SUBMIT_TO_ADDRESS = "submitToAddress";
    public static final String ADD_ALL = "addAll";
    public static final String ADD_ITEM_LISTENER = "addItemListener";
    public static final String RETAIN_ALL = "retainAll";
    public static final String CONTAINS_ALL = "containsAll";
    public static final String CONTAINS = "contains";
    public static final String INDEX_OF = "indexOf";
    public static final String LAST_INDEX_OF = "lastIndexOf";
    public static final String LIST_ITERATOR = "listIterator";
    public static final String REMOVE_ITEM_LISTENER = "removeItemListener";
    public static final String SUB_LIST = "subList";
    public static final String READ_METRICS = "readMetrics";
    public static final String CONTAINS_ENTRY = "containsEntry";
    public static final String VALUE_COUNT = "valueCount";
    public static final String DRAIN_TO = "drainTo";
    public static final String OFFER = "offer";
    public static final String PEEK = "peek";
    public static final String POLL = "poll";
    public static final String REMAINING_CAPACITY = "remainingCapacity";
    public static final String TAKE = "take";
    public static final String CAPACITY = "capacity";
    public static final String HEAD_SEQUENCE = "headSequence";
    public static final String READ_MANY = "readMany";
    public static final String READ_ONE = "readOne";
    public static final String TAIL_SEQUENCE = "tailSequence";
    public static final String GET_ALL_SCHEDULED_FUTURES = "getAllScheduledFutures";
    public static final String SCHEDULE_ON_PARTITION = "scheduleOnPartition";
    public static final String SCHEDULE_ON_MEMBER = "scheduleOnMember";
    public static final String DISPOSE = "dispose";
    public static final String GET_DELAY = "getDelay";
    public static final String GET_RESULT = "getResult";
    public static final String GET_STATISTICS = "getStatistics";
    public static final String IS_CANCELLED = "isCancelled";
    public static final String IS_DONE = "isDone";
    public static final String ADD_MESSAGE_LISTENER = "addMessageListener";
    public static final String PUBLISH_ALL = "publishAll";
    public static final String PUBLISH = "publish";
    public static final String REMOVE_MESSAGE_LISTENER = "removeMessageListener";
    public static final String COMMIT_TRANSACTION = "commitTransaction";
    public static final String BEGIN_TRANSACTION = "beginTransaction";
    public static final String ROLLBACK_TRANSACTION = "rollbackTransaction";
    public static final String CLEAR_REMOTE_TRANSACTION = "clearRemoteTransaction";
    public static final String RECOVER = "recover";
    public static final String FINALIZE_REMOTE_TRANSACTION = "finalizeRemoteTransaction";
    public static final String COMMIT = "commit";
    public static final String START = "start";
    public static final String PREPARE = "prepare";
    public static final String ROLLBACK = "rollback";
    public static final String GET_FOR_UPDATE = "getForUpdate";
    public static final String CREATE_PROXIES = "createProxies";
    public static final String CREATE = "create";
    public static final String DEPLOY_CLASSES = "deployClasses";
    public static final String GET_DISTRIBUTED_OBJECTS = "getDistributedObjects";
    public static final String ADD_DISTRIBUTED_OBJECT_LISTENER = "addDistributedObjectListener";
    public static final String REMOVE_DISTRIBUTED_OBJECT_LISTENER = "removeDistributedObjectListener";
    public static final String COMPARE_AND_SET = "compareAndSet";

    private SecurityInterceptorConstants() {
    }
}
