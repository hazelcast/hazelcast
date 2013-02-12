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

package com.hazelcast.core;

import java.util.EventObject;

/**
 * Map Entry event.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry
 * @see com.hazelcast.core.EntryListener
 * @see com.hazelcast.core.IMap#addEntryListener(EntryListener, boolean)
 */
public class EntryEvent<K, V> extends EventObject {

    private static final long serialVersionUID = -2296203982913729851L;

    public static final int TYPE_ADDED = EntryEventType.ADDED.getType();

    public static final int TYPE_REMOVED = EntryEventType.REMOVED.getType();

    public static final int TYPE_UPDATED = EntryEventType.UPDATED.getType();

    public static final int TYPE_EVICTED = EntryEventType.EVICTED.getType();

    protected final EntryEventType entryEventType;

    protected K key;

    protected V oldValue;

    protected V value;

    protected final Member member;

    protected final String name;

    public EntryEvent(Object source, Member member, int eventType, K key, V value) {
        this(source, member, eventType, key, null, value);
    }

    public EntryEvent(Object source, Member member, int eventType, K key, V oldValue, V value) {
        super(source);
        this.name = (String) source;
        this.member = member;
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
        this.entryEventType = EntryEventType.getByType(eventType);
    }

    @Override
    public Object getSource() {
        return name;
    }

    /**
     * Returns the key of the entry event
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Returns the old value of the entry event
     *
     * @return
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Returns the value of the entry event
     *
     * @return
     */
    public V getValue() {
        return value;
    }

    /**
     * Returns the member fired this event.
     *
     * @return the member fired this event.
     */
    public Member getMember() {
        return member;
    }

    /**
     * Return the event type
     *
     * @return event type
     */
    public EntryEventType getEventType() {
        return entryEventType;
    }

    /**
     * Returns the name of the map for this event.
     *
     * @return name of the map.
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "EntryEvent {" + getSource()
                + "} key=" + getKey()
                + ", oldValue=" + getOldValue()
                + ", value=" + getValue()
                + ", event=" + entryEventType
                + ", by " + member;
    }
}
