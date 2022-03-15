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

package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AnchorDataListHolder {
    private List<Integer> anchorPageList;
    private List<Map.Entry<Data, Data>> anchorDataList;

    public AnchorDataListHolder(List<Integer> anchorPageList, List<Map.Entry<Data, Data>> anchorDataList) {
        this.anchorPageList = anchorPageList;
        this.anchorDataList = anchorDataList;
    }

    public List<Integer> getAnchorPageList() {
        return anchorPageList;
    }

    public List<Map.Entry<Data, Data>> getAnchorDataList() {
        return anchorDataList;
    }

    public <K, V> List<Map.Entry<Integer, Map.Entry<K, V>>> asAnchorList(SerializationService serializationService) {
        List<Map.Entry<Integer, Map.Entry<K, V>>> anchorObjectList = new ArrayList<>(anchorDataList.size());
        Iterator<Map.Entry<Data, Data>> dataEntryIterator = anchorDataList.iterator();
        for (Integer pageNumber : anchorPageList) {
            Map.Entry<Data, Data> dataEntry = dataEntryIterator.next();
            K key = serializationService.toObject(dataEntry.getKey());
            V value = serializationService.toObject(dataEntry.getValue());
            AbstractMap.SimpleImmutableEntry<K, V> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
            anchorObjectList.add(new AbstractMap.SimpleImmutableEntry<>(pageNumber, entry));
        }

        return anchorObjectList;
    }

    public static AnchorDataListHolder of(List<Map.Entry<Integer, Map.Entry>> anchorList,
                                          SerializationService serializationService) {
        List<Map.Entry<Data, Data>> anchorDataList = new ArrayList<>(anchorList.size());
        List<Integer> pageList = new ArrayList<>(anchorList.size());
        anchorList.forEach(item -> {
            pageList.add(item.getKey());
            Map.Entry anchorEntry = item.getValue();
            anchorDataList.add(new AbstractMap.SimpleImmutableEntry<>(serializationService.toData(anchorEntry.getKey()),
                    serializationService.toData(anchorEntry.getValue())));
        });

        return new AnchorDataListHolder(pageList, anchorDataList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnchorDataListHolder that = (AnchorDataListHolder) o;
        return anchorPageList.equals(that.anchorPageList) && anchorDataList.equals(that.anchorDataList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(anchorPageList, anchorDataList);
    }
}
