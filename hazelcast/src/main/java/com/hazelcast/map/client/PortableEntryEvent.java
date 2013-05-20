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

package com.hazelcast.map.client;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.DataAwareEntryEvent;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public final class PortableEntryEvent implements Portable {

    private EntryEvent event;

    public PortableEntryEvent() {
    }

    public PortableEntryEvent(EntryEvent event) {
        this.event = (DataAwareEntryEvent) event;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public EntryEvent getEvent() {
        return event;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.PORTABLE_ENTRY_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        DataAwareEntryEvent dataAwareEvent = (DataAwareEntryEvent) event;
        writer.writeInt("type", dataAwareEvent.getEventType().getType());
        final MemberImpl member = (MemberImpl) dataAwareEvent.getMember();
        writer.writeUTF("host", member.getAddress().getHost());
        writer.writeInt("port", member.getAddress().getPort());
        writer.writeUTF("uuid", member.getUuid());
        writer.writeBoolean("local", member.localMember());
        writer.writeUTF("source", dataAwareEvent.getSource().toString());
        final ObjectDataOutput out = writer.getRawDataOutput();
        dataAwareEvent.getKeyData().writeData(out);
        IOUtil.writeNullableData(out, dataAwareEvent.getOldValueData());
        IOUtil.writeNullableData(out, dataAwareEvent.getNewValueData());
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int type = reader.readInt("type");
        String host = reader.readUTF("host");
        int port = reader.readInt("port");
        String uuid = reader.readUTF("uuid");
        boolean localMember = reader.readBoolean("local");
        String source = reader.readUTF("source");
        Address address = new Address(host, port);
        MemberImpl member = new MemberImpl(address, localMember, uuid);
        final ObjectDataInput in = reader.getRawDataInput();
        Data key = new Data();
        key.readData(in);
        Data oldValue = IOUtil.readNullableData(in);
        Data newValue = IOUtil.readNullableData(in);
        event = new EntryEvent(source, member, type, key, oldValue, newValue);
    }
}
