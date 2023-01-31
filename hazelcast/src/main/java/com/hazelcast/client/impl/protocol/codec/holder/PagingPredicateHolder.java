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

package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.MultiPartitionPredicateImpl;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.query.impl.predicates.PartitionPredicateImpl;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class PagingPredicateHolder {
    private final AnchorDataListHolder anchorDataListHolder;
    private final Data predicateData;
    private final Data comparatorData;
    private final int pageSize;
    private final int page;
    private final byte iterationTypeId;
    private final Collection<Data> partitionKeysData;

    public PagingPredicateHolder(AnchorDataListHolder anchorDataListHolder, Data predicateData, Data comparatorData,
                                 int pageSize, int page, byte iterationTypeId,
                                 Data partitionKeyData,
                                 boolean partitionKeysDataExists,
                                 Collection<Data> partitionKeysData) {
        this.anchorDataListHolder = anchorDataListHolder;
        this.predicateData = predicateData;
        this.comparatorData = comparatorData;
        this.pageSize = pageSize;
        this.page = page;
        this.iterationTypeId = iterationTypeId;
        this.partitionKeysData = partitionKeysData != null
             ? partitionKeysData.stream().collect(Collectors.toSet())
             : partitionKeyData != null
                   ? Collections.singleton(partitionKeyData)
                   : null;
    }

    public AnchorDataListHolder getAnchorDataListHolder() {
        return anchorDataListHolder;
    }

    public Data getPredicateData() {
        return predicateData;
    }

    public Data getComparatorData() {
        return comparatorData;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getPage() {
        return page;
    }

    public byte getIterationTypeId() {
        return iterationTypeId;
    }

    public Data getPartitionKeyData() {
        if (partitionKeysData == null) {
            return null;
        }
        // PagingPredicateHolder constructor ensures that partitionKeysData is non-empty
        return partitionKeysData.iterator().next();
    }

    public Collection<Data> getPartitionKeysData() {
        return partitionKeysData;
    }

    public <K, V> Predicate<K, V> asPredicate(SerializationService serializationService) {
        List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList = anchorDataListHolder.asAnchorList(serializationService);
        Predicate predicate = serializationService.toObject(predicateData);
        Comparator comparator = serializationService.toObject(comparatorData);
        IterationType iterationType = IterationType.getById(iterationTypeId);
        PagingPredicateImpl<K, V> pagingPredicate = new PagingPredicateImpl<K, V>(anchorList, predicate, comparator, pageSize,
                page, iterationType);
        if (partitionKeysData == null) {
            return pagingPredicate;
        }

        Set<? extends Object> partitionKeys = new HashSet<>();
        for (Data keyData : partitionKeysData) {
            partitionKeys.add(serializationService.toObject(keyData));
        }
        
        return partitionKeys.size() == 1 
            ? new PartitionPredicateImpl<>(partitionKeys.iterator().next(), pagingPredicate)
            : new MultiPartitionPredicateImpl<>(partitionKeys, pagingPredicate);
    }

    public static <K, V> PagingPredicateHolder of(Predicate<K, V> predicate,
                                                  SerializationService serializationService) {
        if (predicate instanceof PartitionPredicate) {
            return ofInternal((PartitionPredicate<K, V>) predicate, serializationService);
        }

        return ofInternal((PagingPredicateImpl<K, V>) predicate, serializationService);
    }

    private static <K, V> PagingPredicateHolder ofInternal(PagingPredicateImpl<K, V> pagingPredicate,
                                                           SerializationService serializationService) {
        if (pagingPredicate == null) {
            return null;
        }

        return buildHolder(serializationService, pagingPredicate, null);
    }

    private static <K, V> PagingPredicateHolder ofInternal(@Nonnull PartitionPredicate<K, V> partitionPredicate,
                                                           SerializationService serializationService) {
        PagingPredicateImpl<K, V> pagingPredicate = (PagingPredicateImpl<K, V>) partitionPredicate.getTarget();
        return buildHolder(serializationService,
                pagingPredicate,
                partitionPredicate.getPartitionKeys());
    }

    private static <K, V> PagingPredicateHolder buildHolder(SerializationService serializationService,
                                                            PagingPredicateImpl<K, V> pagingPredicate,
                                                            Collection<? extends Object> partitionKeys) {
        List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList = pagingPredicate.getAnchorList();
        List<Map.Entry<Data, Data>> anchorDataList = new ArrayList<>(anchorList.size());
        List<Integer> pageList = new ArrayList<>(anchorList.size());
        anchorList.forEach(item -> {
            pageList.add(item.getKey());
            Map.Entry<K, V> anchorEntry = item.getValue();
            anchorDataList.add(new AbstractMap.SimpleImmutableEntry<>(serializationService.toData(anchorEntry.getKey()),
                    serializationService.toData(anchorEntry.getValue())));
        });

        AnchorDataListHolder anchorDataListHolder = new AnchorDataListHolder(pageList, anchorDataList);
        Data predicateData = serializationService.toData(pagingPredicate.getPredicate());
        Data comparatorData = serializationService.toData(pagingPredicate.getComparator());
        boolean partitionKeysDataExists = partitionKeys != null;
        return new PagingPredicateHolder(anchorDataListHolder, predicateData, comparatorData, pagingPredicate.getPageSize(),
                pagingPredicate.getPage(), pagingPredicate.getIterationType().getId(), null,
                partitionKeysDataExists,
                partitionKeysDataExists
                    ? partitionKeys.stream().map(k -> (Data) serializationService.toData(k)).collect(Collectors.toList())
                    : null
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PagingPredicateHolder that = (PagingPredicateHolder) o;
        return pageSize == that.pageSize && page == that.page && iterationTypeId == that.iterationTypeId && Objects
                .equals(anchorDataListHolder, that.anchorDataListHolder) && predicateData.equals(that.predicateData)
                && comparatorData.equals(that.comparatorData) && partitionKeysData.equals(that.partitionKeysData);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(anchorDataListHolder, predicateData, comparatorData, pageSize, page, iterationTypeId, partitionKeysData);
    }
}
