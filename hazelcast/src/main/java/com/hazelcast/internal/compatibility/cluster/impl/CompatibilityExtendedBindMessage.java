/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Compatibility extended bind message (sent from a 3.x member) that is,
 * for all intents and purposes, equal to the
 * {@link com.hazelcast.internal.cluster.impl.MemberHandshake}. The difference
 * is in the serialization format and some fields.
 */
public class CompatibilityExtendedBindMessage implements IdentifiedDataSerializable {

    private byte schemaVersion;
    private Map<ProtocolType, Collection<Address>> localAddresses;
    private Address targetAddress;
    private boolean reply;

    public CompatibilityExtendedBindMessage() {
    }

    public CompatibilityExtendedBindMessage(byte schemaVersion, Map<ProtocolType, Collection<Address>> localAddresses,
                                            Address targetAddress, boolean reply) {
        this.schemaVersion = schemaVersion;
        this.localAddresses = new EnumMap<>(localAddresses);
        this.targetAddress = targetAddress;
        this.reply = reply;
    }

    public int getPlaneCount() {
        // multiple planes not supported for 3.x compatibility connections
        return 1;
    }

    public int getPlaneIndex() {
        // multiple planes not supported for 3.x compatibility connections
        return 0;
    }

    public byte getSchemaVersion() {
        return schemaVersion;
    }

    public Map<ProtocolType, Collection<Address>> getLocalAddresses() {
        return localAddresses;
    }

    public Address getTargetAddress() {
        return targetAddress;
    }

    public boolean isReply() {
        return reply;
    }

    @Override
    public int getFactoryId() {
        return CompatibilityClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CompatibilityClusterDataSerializerHook.EXTENDED_BIND_MESSAGE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(schemaVersion);
        out.writeObject(targetAddress);
        out.writeBoolean(reply);
        int size = (localAddresses == null) ? 0 : localAddresses.size();
        out.writeInt(size);
        if (size == 0) {
            return;
        }
        for (Map.Entry<ProtocolType, Collection<Address>> addressEntry : localAddresses.entrySet()) {
            out.writeInt(addressEntry.getKey().ordinal());
            writeCollection(addressEntry.getValue(), out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        schemaVersion = in.readByte();
        targetAddress = in.readObject();
        reply = in.readBoolean();
        int size = in.readInt();
        if (size == 0) {
            localAddresses = Collections.emptyMap();
            return;
        }
        Map<ProtocolType, Collection<Address>> addressesPerProtocolType = new EnumMap<>(ProtocolType.class);
        for (int i = 0; i < size; i++) {
            ProtocolType protocolType = ProtocolType.valueOf(in.readInt());
            Collection<Address> addresses = readCollection(in);
            addressesPerProtocolType.put(protocolType, addresses);
        }
        this.localAddresses = addressesPerProtocolType;
    }

    @Override
    public String toString() {
        return "ExtendedBindMessage{" + "schemaVersion=" + schemaVersion + ", localAddresses=" + localAddresses
                + ", targetAddress=" + targetAddress + ", reply=" + reply + '}';
    }
}
