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

package com.hazelcast.collection.impl.common;

import com.hazelcast.collection.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;

/**
 * An {@link ItemEvent} that is able to store a {@link Data}.
 */
public class DataAwareItemEvent<E> extends ItemEvent<E> {

    private static final long serialVersionUID = 1;

    private final transient Data dataItem;
    private final transient SerializationService serializationService;

    public DataAwareItemEvent(String name, ItemEventType itemEventType, Data dataItem, Member member,
                              SerializationService serializationService) {
        super(name, itemEventType, null, member);
        this.dataItem = dataItem;
        this.serializationService = serializationService;
    }

    @Override
    public E getItem() {
        if (item == null && dataItem != null) {
            item = serializationService.toObject(dataItem);
        }
        return item;
    }

    public Data getItemData() {
        return dataItem;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        throw new NotSerializableException();
    }
}
