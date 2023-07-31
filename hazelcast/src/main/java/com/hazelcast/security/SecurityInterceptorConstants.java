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

public class SecurityInterceptorConstants {
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
    public static final String FETCH = "fetch";
    public static final String REGISTER_CACHE_ENTRY_LISTENER = "registerCacheEntryListener";
    public static final String DEREGISTER_CACHE_ENTRY_LISTENER = "deregisterCacheEntryListener";
    public static final String LOAD_ALL = "loadAll";
    public static final String ENABLE_MANAGEMENT = "enableManagement";
    public static final String PUT_ALL = "putAll";
    public static final String PUT_IF_ABSENT = "putIfAbsent";
    public static final String PUT = "put";
    public static final String GET_AND_PUT = "getAndPut";
    public static final String REMOVE = "remove";
    public static final String REMOVE_ALL_KEYS = "removeAllKeys";
    public static final String REMOVE_ALL = "removeAll";
    public static final String REPLACE = "replace";
    public static final String SET_EXPIRY_POLICY = "setExpiryPolicy";
    public static final String SIZE = "size";
}
