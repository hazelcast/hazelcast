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

package com.hazelcast.core;

import java.util.EventObject;

public class ItemEvent<E> extends EventObject {

    private final E item;
    private final ItemEventType eventType;

    public ItemEvent(String name, int eventType, E item) {
        this(name, ItemEventType.getByType(eventType), item);
    }

    public ItemEvent(String name, ItemEventType itemEventType, E item) {
        super(name);
        this.item = item;
        this.eventType = itemEventType;
    }

    public ItemEventType getEventType() {
        return eventType;
    }

    public E getItem() {
        return item;
    }

    @Override
    public String toString() {
        return "ItemEvent{" +
                "event=" + eventType +
                ", item=" + item +
                "} ";
    }
}
