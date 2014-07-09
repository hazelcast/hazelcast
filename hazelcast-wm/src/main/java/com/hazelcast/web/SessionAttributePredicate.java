/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * Filters map entries to retrieve only the ones that belong to a specific session.
 */
public class SessionAttributePredicate implements Predicate, IdentifiedDataSerializable {

    private String sessionId;

    // Serialization Constructor
    public SessionAttributePredicate() {
    }

    public SessionAttributePredicate(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public boolean apply(Entry mapEntry) {
        Object key = mapEntry.getKey();
        if (key instanceof String) {
            String k = (String) key;
            return k.startsWith(sessionId + WebFilter.HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR);
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sessionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sessionId = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return WebDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WebDataSerializerHook.SESSION_ATTRIBUTE_ID;
    }
}
