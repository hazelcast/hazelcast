/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializerManager;

import static com.hazelcast.nio.IOUtil.toObject;

public class DataAwareItemEvent extends ItemEvent {
    final Data itemData;
    private final transient SerializerManager serializerManager;

    public DataAwareItemEvent(String name, ItemEventType itemEventType, Data itemData,
                              SerializerManager serializerManager) {
        super(name, itemEventType, null);
        this.itemData = itemData;
        this.serializerManager = serializerManager;
    }

    public Data getItemData() {
        return itemData;
    }

    @Override
    public Object getItem() {
        if (serializerManager != null) {
            ThreadContext.get().setCurrentSerializerManager(serializerManager);
        }
        return toObject(itemData);
    }
}
