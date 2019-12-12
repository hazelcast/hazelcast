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

package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class PagingPredicateHolder {
    private AnchorDataListHolder anchorDataListHolder;
    private Data predicateData;
    private Data comparatorData;
    private int pageSize;
    private int page;
    private byte iterationTypeId;

    public PagingPredicateHolder(AnchorDataListHolder anchorDataListHolder, Data predicateData, Data comparatorData, int pageSize,
                                 int page, byte iterationTypeId) {
        this.anchorDataListHolder = anchorDataListHolder;
        this.predicateData = predicateData;
        this.comparatorData = comparatorData;
        this.pageSize = pageSize;
        this.page = page;
        this.iterationTypeId = iterationTypeId;
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

    public <K, V> PagingPredicateImpl<K, V> asPagingPredicate(SerializationService serializationService) {

        List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList = anchorDataListHolder.asAnchorList(serializationService);
        Predicate predicate = serializationService.toObject(predicateData);
        Comparator comparator = serializationService.toObject(comparatorData);
        IterationType iterationType = IterationType.getById(iterationTypeId);
        return new PagingPredicateImpl<K, V>(anchorList, predicate, comparator, pageSize, page, iterationType);
    }

    public static <K, V> PagingPredicateHolder of(PagingPredicateImpl<K, V> pagingPredicate,
                                                  SerializationService serializationService) {
        if (pagingPredicate == null) {
            return null;
        }

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
        return new PagingPredicateHolder(anchorDataListHolder, predicateData, comparatorData, pagingPredicate.getPageSize(),
                pagingPredicate.getPage(), pagingPredicate.getIterationType().getId());
    }

}
