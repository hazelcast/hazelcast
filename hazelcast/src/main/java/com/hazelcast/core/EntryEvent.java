/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.util.EventObject;

public class EntryEvent<K, V> extends EventObject {

    private static final long serialVersionUID = -2296203982913729851L;

    public static final int TYPE_ADDED = 1;

    public static final int TYPE_REMOVED = 2;

    public static final int TYPE_UPDATED = 3;

    public static final int TYPE_EVICTED = 4;

    protected EntryEventType entryEventType = EntryEventType.ADDED;

    protected K key;

    protected V value;

    protected final String name;

    public enum EntryEventType {
        ADDED(1),
        REMOVED(2),
        UPDATED(3),
        EVICTED(4);
        private int type;

        EntryEventType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }
    }

    protected boolean collection;

    public EntryEvent(Object source) {
        super(source);
        this.name = (String) source;
        collection = (name.charAt(0) == 't') || (name.charAt(0) == 'q') || (name.charAt(3) == ':');
    }

    public EntryEvent(Object source, int eventType, K key, V value) {
        this(source);
        this.key = key;
        this.value = value;
        if (eventType == TYPE_REMOVED) {
            entryEventType = EntryEventType.REMOVED;
        } else if (eventType == TYPE_UPDATED) {
            entryEventType = EntryEventType.UPDATED;
        } else if (eventType == TYPE_EVICTED) {
            entryEventType = EntryEventType.EVICTED;
        }
    }

    @Override
    public Object getSource() {
        return name;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public EntryEventType getEventType() {
        return entryEventType;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "EntryEvent {" + getSource() + "} key="
                + getKey() + ", value=" + getValue() + ", event="
                + entryEventType;
    }
}
