/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * MemberHandshake message, conveying information about all kinds of public
 * addresses per protocol type.
 * It is the first message exchanged on a new connection
 * so {@link com.hazelcast.nio.serialization.impl.Versioned Versioned}
 * serialization cannot be used as there may be no cluster version
 * established yet. The {@code MemberHandshake} itself includes a
 * schema version so it can be extended in future versions without having
 * to use another packet type.
 *
 * <h1>Options</h1>
 * Since 4.1 it is possible to add options to the MemberHandshake without breaking
 * the protocol in the form of options which effectively is a map with keys and values
 * of type string.
 *
 * @since 3.12
 */
public class MemberHandshake
        implements IdentifiedDataSerializable {

    public static final byte SCHEMA_VERSION_1 = (byte) 1;
    public static final byte SCHEMA_VERSION_2 = (byte) 2;

    public static final String OPTION_PLANE_COUNT = "planeCount";
    public static final String OPTION_PLANE_INDEX = "planeIndex";

    private byte schemaVersion;
    private Map<ProtocolType, Collection<Address>> localAddresses;
    private Address targetAddress;
    private boolean reply;
    private UUID uuid;
    private final Map<String, String> options = new HashMap<>();

    public MemberHandshake() {
    }

    public MemberHandshake(byte schemaVersion,
                           Map<ProtocolType, Collection<Address>> localAddresses,
                           Address targetAddress,
                           boolean reply,
                           UUID uuid) {
        this.schemaVersion = schemaVersion;
        this.localAddresses = new EnumMap<>(localAddresses);
        this.targetAddress = targetAddress;
        this.reply = reply;
        this.uuid = uuid;
    }

    public MemberHandshake addOption(String key, Object value) {
        options.put(key, "" + value);
        return this;
    }

    public int getIntOption(String key, int defaultValue) {
        String value = options.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public int getPlaneCount() {
        return getIntOption(OPTION_PLANE_COUNT, 1);
    }

    public int getPlaneIndex() {
        return getIntOption(OPTION_PLANE_INDEX, 0);
    }

    byte getSchemaVersion() {
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

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER_HANDSHAKE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(schemaVersion);
        out.writeObject(targetAddress);
        out.writeBoolean(reply);
        writeUUID(out, uuid);
        int size = localAddresses == null ? 0 : localAddresses.size();
        out.writeInt(size);
        if (size > 0) {
            for (Map.Entry<ProtocolType, Collection<Address>> addressEntry : localAddresses.entrySet()) {
                out.writeInt(addressEntry.getKey().ordinal());
                writeCollection(addressEntry.getValue(), out);
            }
        }

        int optionsSize = options.size();
        out.writeInt(optionsSize);
        if (optionsSize > 0) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        schemaVersion = in.readByte();
        targetAddress = in.readObject();
        reply = in.readBoolean();
        uuid = readUUID(in);
        int size = in.readInt();
        if (size == 0) {
            localAddresses = Collections.emptyMap();
        } else {
            Map<ProtocolType, Collection<Address>> addressesPerProtocolType = new EnumMap<>(ProtocolType.class);
            for (int i = 0; i < size; i++) {
                ProtocolType protocolType = ProtocolType.valueOf(in.readInt());
                Collection<Address> addresses = readCollection(in);
                addressesPerProtocolType.put(protocolType, addresses);
            }
            this.localAddresses = addressesPerProtocolType;
        }

        if (schemaVersion > SCHEMA_VERSION_1) {
            int optionsSize = in.readInt();
            for (int k = 0; k < optionsSize; k++) {
                options.put(in.readString(), in.readString());
            }
        }
    }

    @Override
    public String toString() {
        return "MemberHandshake{"
                + "schemaVersion=" + schemaVersion
                + ", localAddresses=" + localAddresses
                + ", targetAddress=" + targetAddress
                + ", reply=" + reply
                + ", uuid=" + uuid
                + ", options=" + options
                + '}';
    }
}
