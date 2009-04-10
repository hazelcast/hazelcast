/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.FactoryImpl;

import java.util.EventObject;

public class EntryEvent extends EventObject {

    public static final int TYPE_ADDED = 1;

    public static final int TYPE_REMOVED = 2;

    public static final int TYPE_UPDATED = 3;

    protected int eventType;

    protected Object key;

    protected Object value;

    protected final String name;

    private final static String ADDED = "added";
    private final static String REMOVED = "removed";
    private final static String UPDATED = "updated";

    protected boolean collection;

    public EntryEvent(Object source) {
        super(source);
        this.name = (String) source;
        if (name.charAt(0) == 't' || name.charAt(0) == 'q' || name.charAt(3) == ':') {
            collection = true;
        } else
            collection = false;

    }

    public EntryEvent(Object source, int eventType, Object key, Object value) {
        this(source);
        this.eventType = eventType;
        this.key = key;
        this.value = value;
    }

    @Override
    public Object getSource() {
        if (name.startsWith("q:t:")) {
            return FactoryImpl.getProxy(name.substring(2));
        }
        return FactoryImpl.getProxy(name);
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public int getEventType() {
        return eventType;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        String event = ADDED;
        if (eventType == TYPE_REMOVED) {
            event = REMOVED;
        } else if (eventType == TYPE_UPDATED) {
            event = UPDATED;
        }
        return "EntryEvent {" + getSource() + "} key=" + key + ", value=" + value + ", event="
                + event;

    }
}
