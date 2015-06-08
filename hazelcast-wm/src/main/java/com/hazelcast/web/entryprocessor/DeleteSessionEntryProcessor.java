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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.web.JvmIdAware;
import com.hazelcast.web.SessionState;
import com.hazelcast.web.WebDataSerializerHook;

import java.io.IOException;
import java.util.Map;

/**
 * Entry processor which removes SessionState values if
 * invalidate is true. See DeleteSessionEntryProcessor.process
 */

public final class DeleteSessionEntryProcessor
        implements EntryProcessor<String, SessionState>,
        EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable, JvmIdAware {

    private String jvmId;
    private boolean invalidate;
    private boolean removed;

    public DeleteSessionEntryProcessor(String jvmId, boolean invalidate) {
        this.jvmId = jvmId;
        this.invalidate = invalidate;
    }

    public DeleteSessionEntryProcessor() {
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
        return WebDataSerializerHook.SESSION_DELETE;
    }

    @Override
    public Object process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return Boolean.FALSE;
        }
        sessionState.removeJvmId(jvmId);
        if (invalidate) {
            entry.setValue(null);
            removed = true;
        } else {
            entry.setValue(sessionState);
        }
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return (removed) ? this : null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(jvmId);
        out.writeBoolean(invalidate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jvmId = in.readUTF();
        invalidate = in.readBoolean();
    }

    @Override
    public void processBackup(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState != null) {
            entry.setValue(null);
        }
    }
}
