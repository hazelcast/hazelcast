/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

/**
 * Client Protocol Event Responses
 */
@GenerateCodec(id = 0, name = "Event", ns = "")
public interface EventResponseTemplate {

    /**
     *
     * @param member Cluster member server
     * @param eventType Type of the event. The possible values are:
     *                  1: Member added event.
     *                  2: Member removed event.
     */
    @EventResponse(EventMessageConst.EVENT_MEMBER)
    void Member(Member member, int eventType);

    /**
     * @param uuid          Unique user id of the member serve
     * @param key           Name of the attribute changed
     * @param operationType Type of the change. Possible values are:
     *                      1: An attribute is added
     *                      2: An attribute is removed
     * @param value         Value of the attribute. This field only exist for operation type of 1,
     *                      otherwise this field is not transferred at all
     */
    @EventResponse(EventMessageConst.EVENT_MEMBERATTRIBUTECHANGE)
    void MemberAttributeChange(String uuid, String key, int operationType, @Nullable String value);

    /**
     *
     * @param members The list of members in the cluster. It is used to retrieve the initial list in the cluster when the client
     *                registers for the membership events.
     */
    @EventResponse(EventMessageConst.EVENT_MEMBERSET)
    void MemberSet(Set<Member> members);

    /**
     *
     * @param key The key for the entry in the map.
     * @param value The value of the entry in the map.
     * @param oldValue The original value for the key in the map if exists.
     * @param mergingValue The incoming merging value of the entry event.
     * @param eventType Possible types are:
     *                      ADDED(1)
                            REMOVED(2)
                            UPDATED(3)
                            EVICTED(4)
                            EVICT_ALL(5)
                            CLEAR_ALL(6)
                            MERGED(7)
     * @param uuid The id of the member.
     * @param numberOfAffectedEntries The number of entries affected in the map.
     */
    @EventResponse(EventMessageConst.EVENT_ENTRY)
    void Entry(@Nullable Data key, @Nullable Data value, @Nullable Data oldValue, @Nullable Data mergingValue, int eventType,
               String uuid, int numberOfAffectedEntries);

    /**
     *
     * @param item Map data item.
     * @param uuid The id of the server member.
     * @param eventType There are two possible values:
     *                  1: ADDED
     *                  2: REMOVED
     */
    @EventResponse(EventMessageConst.EVENT_ITEM)
    void Item(@Nullable Data item, String uuid, int eventType);

    /**
     *
     * @param item The published item
     * @param publishTime The time the topic is published.
     * @param uuid The id of the server member.
     */
    @EventResponse(EventMessageConst.EVENT_TOPIC)
    void Topic(Data item, long publishTime, String uuid);

    /**
     *
     * @param partitionId The lost partition id.
     * @param lostBackupCount The number of lost backups for the partition. O: the owner, 1: first backup, 2: second backup ...
     * @param source The address of the node that dispatches the event.
     */
    @EventResponse(EventMessageConst.EVENT_PARTITIONLOST)
    void PartitionLost(int partitionId, int lostBackupCount, @Nullable Address source);

    /**
     *
     * @param name Name of the DistributedObject instance
     * @param serviceName Name of the service.
     * @param eventType Can be one of the two values:
     *                  "CREATED"
     *                  "DESTROYED"
     */
    @EventResponse(EventMessageConst.EVENT_DISTRIBUTEDOBJECT)
    void DistributedObject(String name, String serviceName, String eventType);

    /**
     *
     * @param name Name of the cache.
     * @param key The key for the entry.
     * @param sourceUuid The id of the server member.
     */
    @EventResponse(EventMessageConst.EVENT_CACHEINVALIDATION)
    void CacheInvalidation(String name, @Nullable Data key, @Nullable String sourceUuid);

    /**
     *
     * @param name Name of the cache.
     * @param keys The keys for the entries in batch invalidation.
     * @param sourceUuids The ids of the server members.
     */
    @EventResponse(EventMessageConst.EVENT_CACHEBATCHINVALIDATION)
    void CacheBatchInvalidation(String name, List<Data> keys, @Nullable List<String> sourceUuids);

    /**
     *
     * @param partitionId The partition id that has been lost for the given map
     * @param uuid The id of the server member.
     */
    @EventResponse(EventMessageConst.EVENT_MAPPARTITIONLOST)
    void MapPartitionLost(int partitionId, String uuid);

    /**
     *
     * @param type The type of the event. Possible values for the event are:
                    CREATED(1): An event type indicating that the cache entry was created.
                    UPDATED(2): An event type indicating that the cache entry was updated, i.e. a previous mapping existed.
                    REMOVED(3): An event type indicating that the cache entry was removed.
                    EXPIRED(4): An event type indicating that the cache entry has expired.
                    EVICTED(5): An event type indicating that the cache entry has evicted.
                    INVALIDATED(6): An event type indicating that the cache entry has invalidated for near cache invalidation.
                    COMPLETED(7): An event type indicating that the cache operation has completed.
                    EXPIRATION_TIME_UPDATED(8): An event type indicating that the expiration time of cache record has been updated
     * @param keys The keys for the entries in the cache.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     */
    @EventResponse(EventMessageConst.EVENT_CACHE)
    void Cache(int type, Set<CacheEventData> keys, int completionId);

    /**
     *
     * @param data Query cache map event data.
     */
    @EventResponse(EventMessageConst.EVENT_QUERYCACHESINGLE)
    void QueryCacheSingle(QueryCacheEventData data);

    /**
     *
     * @param events Array of query cache events
     * @param source Source of the event.
     * @param partitionId The partition id for the query cache.
     */
    @EventResponse(EventMessageConst.EVENT_QUERYCACHEBATCH)
    void QueryCacheBatch(List<QueryCacheEventData> events, String source, int partitionId);

    @EventResponse(EventMessageConst.EVENT_CACHEPARTITIONLOST)
    void CachePartitionLost(int partitionId, String uuid);

}
