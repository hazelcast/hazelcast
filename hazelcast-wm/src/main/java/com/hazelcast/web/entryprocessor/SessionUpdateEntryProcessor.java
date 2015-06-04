/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web.entryprocessor;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.web.JvmIdAware;
import com.hazelcast.web.SessionState;
import com.hazelcast.web.WebDataSerializerHook;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry processor which updates SessionState attributes stored in distributed map and
 * adds current jvmId into SessionState If value of attribute is set to null. It is removed from
 * SessionState.attribute map.
 * See SessionUpdateEntryProcessor.process
 */

public final class SessionUpdateEntryProcessor
        implements EntryProcessor<String, SessionState>,
        EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable, JvmIdAware {

    private Map<String, Data> attributes;
    private String jvmId;

    public SessionUpdateEntryProcessor(int size) {
        this.attributes = new HashMap<String, Data>(size);
    }

    public SessionUpdateEntryProcessor(String key, Data value) {
        attributes = new HashMap<String, Data>(1);
        attributes.put(key, value);
    }

    public SessionUpdateEntryProcessor() {
        attributes = Collections.emptyMap();
    }

    public Map<String, Data> getAttributes() {
        return attributes;
    }

    public String getJvmId() {
        return jvmId;
    }

    public void setJvmId(String jvmId) {
        this.jvmId = jvmId;
    }

    @Override
    public int getFactoryId() {
        return WebDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WebDataSerializerHook.SESSION_UPDATE;
    }

    @Override
    public Object process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            sessionState = new SessionState();
        }
        sessionState.addJvmId(jvmId);
        for (Map.Entry<String, Data> attribute : attributes.entrySet()) {
            String name = attribute.getKey();
            Data value = attribute.getValue();
            if (value == null) {
                sessionState.getAttributes().remove(name);
            } else {
                sessionState.getAttributes().put(name, value);
            }
        }
        entry.setValue(sessionState);
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(jvmId);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Data> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jvmId = in.readUTF();
        int attCount = in.readInt();
        attributes = new HashMap<String, Data>(attCount);
        for (int i = 0; i < attCount; i++) {
            attributes.put(in.readUTF(), in.readData());
        }
    }

    @Override
    public void processBackup(Map.Entry<String, SessionState> entry) {
        process(entry);
    }
}
