/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

public class DataAwareItemEvent extends ItemEvent {
    final Data itemData;
    private final transient SerializationService serializationService;

    public DataAwareItemEvent(String name, ItemEventType itemEventType, Data itemData, Member member,
                              SerializationService serializationService) {
        super(name, itemEventType, null, member);
        this.itemData = itemData;
        this.serializationService = serializationService;
    }

    public Data getItemData() {
        return itemData;
    }

    @Override
    public Object getItem() {
        return serializationService.toObject(itemData);
    }
}
