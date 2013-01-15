/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.processor;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @ali 1/1/13
 */
public class Entry {

    private Data key;

    private Object value;

    private CollectionContainer container;

    private int threadId = -1;

    private Address caller;

    private EntryEventType eventType;

    private Object eventValue;

    public Entry(CollectionContainer container, Data key, int threadId, Address caller) {
        this.container = container;
        this.key = key;
        this.value = container.getObject(key);
        this.threadId = threadId;
        this.caller = caller;
    }

    public Data getKey() {
        return key;
    }

    public boolean removeEntry() {
        return container.removeObject(key);
    }

    public boolean canAcquireLock(){
        return container.canAcquireLock(key, threadId, caller);
    }

    public boolean isLocked(){
        return container.isLocked(key);
    }

    public boolean lock(long ttl){
        return container.lock(key, caller, threadId, ttl);
    }

    public boolean unlock(){
        return container.unlock(key, caller, threadId);
    }

    public <T> T getOrCreateValue() {
        if (value == null) {
            return (T) container.putNewObject(key);
        }
        return (T) value;
    }

    public <T> T getValue() {
        return (T) value;
    }

    public SerializationService getSerializationService(){
        return container.getNodeEngine().getSerializationService();
    }

    public void publishEvent(EntryEventType eventType, Object eventValue){
        this.eventType = eventType;
        this.eventValue = eventValue;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public Object getEventValue() {
        return eventValue;
    }
}
