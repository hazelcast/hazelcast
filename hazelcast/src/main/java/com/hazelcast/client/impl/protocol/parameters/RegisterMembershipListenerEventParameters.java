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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class RegisterMembershipListenerEventParameters {

    public static final int MEMBER_ADDED = 1;
    public static final int MEMBER_REMOVED = 2;
    public static final int MEMBER_ATTRIBUTE_CHANGED = 3;
    public static final int INITIAL_MEMBERS = 4;

    public static final ClientMessageType TYPE = ClientMessageType.REGISTER_MEMBERSHIP_LISTENER_EVENT;

    public Member member;
    public MemberAttributeChange memberAttributeChange;
    public int eventType;
    public Collection<MemberImpl> memberList;

    private RegisterMembershipListenerEventParameters(ClientMessage flyweight) {
        member = decodeMember(flyweight);
        memberAttributeChange = decodeMemberAttributeChange(flyweight);
        eventType = flyweight.getInt();
        memberList = decodeMemberList(flyweight);

    }

    private LinkedList<MemberImpl> decodeMemberList(ClientMessage flyweight) {
        LinkedList<MemberImpl> members = new LinkedList<MemberImpl>();
        int size = flyweight.getInt();
        for (int i = 0; i < size; i++) {
            members.add(decodeMember(flyweight));
        }
        return members;
    }

    private MemberAttributeChange decodeMemberAttributeChange(ClientMessage flyweight) {
        boolean isNull = flyweight.getBoolean();
        if(isNull){
            return null;
        }
        String uuid = flyweight.getStringUtf8();
        String key = flyweight.getStringUtf8();
        MemberAttributeOperationType operationType = MemberAttributeOperationType.getValue(flyweight.getByte());
        Object value = null;
        if (operationType == PUT) {
            value = decodePrimitive(flyweight);
        }
        return new MemberAttributeChange(uuid, operationType, key, value);
    }

    private MemberImpl decodeMember(ClientMessage flyweight) {
        boolean isNull = flyweight.getBoolean();
        if(isNull){
            return null;
        }
        String host = flyweight.getStringUtf8();
        int port = flyweight.getInt();
        String uuid = flyweight.getStringUtf8();
        int attributeSize = flyweight.getInt();
        Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();
        for (int i = 0; i < attributeSize; i++) {
            String key = flyweight.getStringUtf8();
            Object value = decodePrimitive(flyweight);
            attributes.put(key, value);
        }

        try {
            return new MemberImpl(new Address(host, port), false, uuid, null, attributes);
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private Object decodePrimitive(ClientMessage flyweight) {
        byte primitiveType = flyweight.getByte();
        Object value;
        switch (primitiveType) {
            case IOUtil.PRIMITIVE_TYPE_BOOLEAN:
                value = flyweight.getBoolean();
                break;
            case IOUtil.PRIMITIVE_TYPE_BYTE:
                value = flyweight.getByte();
                break;
            case IOUtil.PRIMITIVE_TYPE_SHORT:
                value = flyweight.getShort();
                break;
            case IOUtil.PRIMITIVE_TYPE_INTEGER:
                value = flyweight.getInt();
                break;
            case IOUtil.PRIMITIVE_TYPE_LONG:
                value = flyweight.getLong();
                break;
            case IOUtil.PRIMITIVE_TYPE_FLOAT:
                value = flyweight.getFloat();
                break;
            case IOUtil.PRIMITIVE_TYPE_DOUBLE:
                value = flyweight.getDouble();
                break;
            case IOUtil.PRIMITIVE_TYPE_UTF:
                value = flyweight.getStringUtf8();
                break;
            default:
                value = null;
        }
        return value;
    }

    public static RegisterMembershipListenerEventParameters decode(ClientMessage flyweight) {
        return new RegisterMembershipListenerEventParameters(flyweight);
    }

    public static ClientMessage encode(Member member, int eventType) {
        return encode(member, null, eventType, Collections.EMPTY_LIST);
    }

    public static ClientMessage encode(Member member, MemberAttributeChange memberAttributeChange) {
        return encode(member, memberAttributeChange, MEMBER_ATTRIBUTE_CHANGED, Collections.EMPTY_LIST);
    }

    public static ClientMessage encode(Collection<MemberImpl> members) {
        return encode(null, null, INITIAL_MEMBERS, members);
    }

    private static ClientMessage encode(Member member, MemberAttributeChange memberAttributeChange, int eventType, Collection<MemberImpl> members) {
        final int requiredDataSize = calculateDataSize(member, memberAttributeChange, eventType, members);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);

        encodeMember(member, clientMessage);
        encodeMemberAttributeChange(memberAttributeChange, clientMessage);
        clientMessage.set(eventType);
        encodeMemberList(members, clientMessage);


        clientMessage.setFlags(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static void encodeMemberList(Collection<MemberImpl> members, ClientMessage clientMessage) {
        clientMessage.set(members.size());
        for (MemberImpl m : members) {
            encodeMember(m, clientMessage);
        }
    }

    private static void encodeMemberAttributeChange(MemberAttributeChange memberAttributeChange, ClientMessage clientMessage) {
        boolean isNull = memberAttributeChange == null;
        clientMessage.set(isNull);
        if(isNull){
            return;
        }
        clientMessage.set(memberAttributeChange.getUuid());
        clientMessage.set(memberAttributeChange.getKey());

        MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
        clientMessage.set(operationType.getId());
        if (operationType == PUT) {
            encodePrimitive(clientMessage, memberAttributeChange.getValue());
        }
    }

    private static void encodeMember(Member member, ClientMessage clientMessage) {
        boolean isNull = member == null;
        clientMessage.set(isNull);
        if(isNull){
            return;
        }
        InetSocketAddress socketAddress = member.getSocketAddress();
        clientMessage.set(socketAddress.getHostName()).set(socketAddress.getPort()).set(member.getUuid());
        Map<String, Object> attributes = member.getAttributes();
        clientMessage.set(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            clientMessage.set(entry.getKey());
            Object value = entry.getValue();
            encodePrimitive(clientMessage, value);

        }
    }

    private static void encodePrimitive(ClientMessage clientMessage, Object value) {
        Class<?> type = value.getClass();
        if (type.equals(Boolean.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_BOOLEAN);
            clientMessage.set((Boolean) value);
        } else if (type.equals(Byte.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_BYTE);
            clientMessage.set((Byte) value);
        } else if (type.equals(Short.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_SHORT);
            clientMessage.set((Short) value);
        } else if (type.equals(Integer.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_INTEGER);
            clientMessage.set((Integer) value);
        } else if (type.equals(Long.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_LONG);
            clientMessage.set((Long) value);
        } else if (type.equals(Float.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_FLOAT);
            clientMessage.set((Float) value);
        } else if (type.equals(Double.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_DOUBLE);
            clientMessage.set((Double) value);
        } else if (type.equals(String.class)) {
            clientMessage.set(IOUtil.PRIMITIVE_TYPE_UTF);
            clientMessage.set((String) value);
        } else {
            throw new IllegalStateException("Illegal attribute type id found");
        }
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Member member, MemberAttributeChange memberAttributeChange, int eventType, Collection<MemberImpl> members) {
        int dataSize = ClientMessage.HEADER_SIZE;
        dataSize += calculateMemberDataSize(member);
        dataSize += calculateMemberAttributeChangeDataSize(memberAttributeChange);
        dataSize += BitUtil.SIZE_OF_INT; //event_type
        dataSize += BitUtil.SIZE_OF_INT; //member list size
        for (MemberImpl m : members) {
            dataSize += calculateMemberDataSize(m);
        }
        return dataSize;
    }

    private static int calculateMemberAttributeChangeDataSize(MemberAttributeChange memberAttributeChange) {
        if(memberAttributeChange == null){
            return BitUtil.SIZE_OF_BOOLEAN;
        }
        int dataSize = ParameterUtil.calculateStringDataSize(memberAttributeChange.getUuid());
        dataSize += ParameterUtil.calculateStringDataSize(memberAttributeChange.getKey());
        dataSize += BitUtil.SIZE_OF_BYTE;
        MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
        if (operationType == PUT) {
            dataSize += calculatePrimitiveDataSize(memberAttributeChange.getValue());
        }
        return dataSize;
    }

    private static int calculateMemberDataSize(Member member) {
        if(member == null){
            return BitUtil.SIZE_OF_BOOLEAN;
        }
        int dataSize = ParameterUtil.calculateStringDataSize(member.getSocketAddress().getHostName());
        dataSize += BitUtil.SIZE_OF_INT; // port
        dataSize += ParameterUtil.calculateStringDataSize(member.getUuid());
        dataSize += BitUtil.SIZE_OF_INT; // attributes size
        Map<String, Object> attributes = member.getAttributes();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            dataSize += ParameterUtil.calculateStringDataSize(entry.getKey());

            Object value = entry.getValue();
            dataSize += calculatePrimitiveDataSize(value);
        }
        return dataSize;
    }

    private static int calculatePrimitiveDataSize(Object value) {
        int dataSize = BitUtil.SIZE_OF_BYTE;

        Class<?> type = value.getClass();
        if (type.equals(Boolean.class)) {
            dataSize += BitUtil.SIZE_OF_BOOLEAN;
        } else if (type.equals(Byte.class)) {
            dataSize += BitUtil.SIZE_OF_BYTE;
        } else if (type.equals(Short.class)) {
            dataSize += BitUtil.SIZE_OF_SHORT;
        } else if (type.equals(Integer.class)) {
            dataSize += BitUtil.SIZE_OF_INT;
        } else if (type.equals(Long.class)) {
            dataSize += BitUtil.SIZE_OF_LONG;
        } else if (type.equals(Float.class)) {
            dataSize += BitUtil.SIZE_OF_FLOAT;
        } else if (type.equals(Double.class)) {
            dataSize += BitUtil.SIZE_OF_DOUBLE;
        } else if (type.equals(String.class)) {
            dataSize += ParameterUtil.calculateStringDataSize((String) value);
        } else {
            throw new IllegalStateException("Illegal attribute type id found");
        }
        return dataSize;
    }
}
