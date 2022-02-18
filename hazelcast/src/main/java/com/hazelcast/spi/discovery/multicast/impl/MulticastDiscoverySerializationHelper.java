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

package com.hazelcast.spi.discovery.multicast.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Helps to serialize and deserialize the {@link MulticastMemberInfo}.
 *
 * @see com.hazelcast.spi.discovery.multicast.MulticastProperties#SAFE_SERIALIZATION
 */
public class MulticastDiscoverySerializationHelper {

    private final boolean safeSerialization;

    public MulticastDiscoverySerializationHelper(boolean safeSerialization) {
        this.safeSerialization = safeSerialization;
    }

    public byte[] serialize(MulticastMemberInfo memberInfo) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if (safeSerialization) {
            try (DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeBoolean(memberInfo != null);
                if (memberInfo != null) {
                    dos.writeUTF(memberInfo.getHost());
                    dos.writeInt(memberInfo.getPort());
                }
            }
        } else {
            try (ObjectOutput oo = new ObjectOutputStream(bos)) {
                oo.writeObject(memberInfo);
            }
        }
        return bos.toByteArray();
    }

    public MulticastMemberInfo deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null) {
            return null;
        }
        MulticastMemberInfo memberInfo = null;
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        if (safeSerialization) {
            try (DataInputStream dis = new DataInputStream(bis)) {
                boolean memberInfoIncluded = dis.readBoolean();
                if (memberInfoIncluded) {
                    String tmpHost = dis.readUTF();
                    int tmpPort = dis.readInt();
                    memberInfo = new MulticastMemberInfo(tmpHost, tmpPort);
                }
            }
        } else {
            try (ObjectInputStream in = new ObjectInputStream(bis)) {
                memberInfo = (MulticastMemberInfo) in.readObject();
            }
        }
        return memberInfo;
    }
}
