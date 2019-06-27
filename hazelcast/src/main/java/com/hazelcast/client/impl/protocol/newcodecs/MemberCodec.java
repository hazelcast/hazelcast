/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.newcodecs;

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;

public class MemberCodec {

    private static int LITE_MEMBER = 0;
    private static int UUID_LOW = LITE_MEMBER + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static int UUID_HIGH = UUID_LOW + Bits.LONG_SIZE_IN_BYTES;
    private static int HEADER_SIZE = UUID_HIGH + Bits.LONG_SIZE_IN_BYTES;

    static void encode(ClientMessage clientMessage, Member member) {
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], DEFAULT_FLAGS);
        initialFrame.content[LITE_MEMBER] = (byte) (member.isLiteMember() ? 1 : 0);

        String uuid = member.getUuid();
        UUID uuid1 = UUID.fromString(uuid);
        Bits.writeLongL(initialFrame.content, UUID_LOW, uuid1.getLeastSignificantBits());
        Bits.writeLongL(initialFrame.content, UUID_HIGH, uuid1.getMostSignificantBits());
        clientMessage.addFrame(initialFrame);

        clientMessage.addFrame(BEGIN_FRAME);
        AddressCodec.encode(clientMessage, member.getAddress());
        clientMessage.addFrame(END_FRAME);

        Map<String, String> attributes = member.getAttributes();
        clientMessage.addFrame(BEGIN_FRAME);
        for (String key : attributes.keySet()) {
            clientMessage.addFrame(new ClientMessage.Frame(key.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
        }
        clientMessage.addFrame(END_FRAME);

        clientMessage.addFrame(BEGIN_FRAME);
        for (String value : attributes.values()) {
            clientMessage.addFrame(new ClientMessage.Frame(value.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
        }
        clientMessage.addFrame(END_FRAME);
    }

    static Member decode(Iterator<ClientMessage.Frame> iterator) {

        byte[] initialFrame = iterator.next().content;
        boolean isLiteMember = initialFrame[LITE_MEMBER] == 1;

        long uuidLow = Bits.readLongL(initialFrame, UUID_LOW);
        long uuidHigh = Bits.readLongL(initialFrame, UUID_HIGH);

        UUID uuid = new UUID(uuidHigh, uuidLow);

        ClientMessage.Frame frame = iterator.next();//begin
        Address address = AddressCodec.decode(iterator);
        for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {

        }

        HashMap<String, String> attributes = new HashMap<>();
        List<ClientMessage.Frame> keys = new LinkedList<>();
        iterator.next();//begin
        for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
            keys.add(frame);
        }

//        iterator.next();//begin
//        List<ClientMessage.Frame> values = new LinkedList<>();
//        while ((frame = iterator.next()) != END_FRAME) {
//            values.add(frame);
//        }


        iterator.next();//begin
        for (ClientMessage.Frame key : keys) {
            ClientMessage.Frame value = iterator.next();
            attributes.put(new String(key.content, Bits.UTF_8), new String(value.content, Bits.UTF_8));
        }
        iterator.next();//end

        return new MemberImpl(address, uuid.toString(), attributes, isLiteMember);


    }
}
