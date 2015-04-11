package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;

public class GetPartitionsResultParameters {

    public static final ClientMessageType TYPE = ClientMessageType.GET_PARTITIONS_RESULT;
    public Address[] members;
    public int[] ownerIndexes;

    private GetPartitionsResultParameters(ClientMessage flyweight) throws UnknownHostException {
        members = decodeAddressArray(flyweight);
        ownerIndexes = decodeIntArray(flyweight);
    }

    private int[] decodeIntArray(ClientMessage flyweight) {
        int size = flyweight.getInt();
        int[] indexes = new int[size];

        for (int i = 0; i < size; i++) {
            indexes[i] = flyweight.getInt();
        }
        return indexes;
    }

    private Address[] decodeAddressArray(ClientMessage flyweight) throws UnknownHostException {
        int size = flyweight.getInt();
        Address[] addresses = new Address[size];
        for (int i = 0; i < size; i++) {
            addresses[i] = ParameterUtil.decodeAddress(flyweight);

        }
        return addresses;
    }

    public static GetPartitionsResultParameters decode(ClientMessage flyweight) throws UnknownHostException {
        return new GetPartitionsResultParameters(flyweight);
    }

    public static ClientMessage encode(Address[] addresses, int[] ownerIndexes) {
        final int requiredDataSize = calculateDataSize(addresses, ownerIndexes);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);

        encodeAddressArray(addresses, clientMessage);
        encodeIntArray(ownerIndexes, clientMessage);


        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static void encodeAddressArray(Address[] addresses, ClientMessage clientMessage) {
        clientMessage.set(addresses.length);
        for (Address address : addresses) {
            ParameterUtil.encodeAddress(clientMessage, address);
        }
    }

    private static void encodeIntArray(int[] ownerIndexes, ClientMessage clientMessage) {
        clientMessage.set(ownerIndexes.length);
        for (int ownerIndex : ownerIndexes) {
            clientMessage.set(ownerIndex);
        }
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Address[] addresses, int[] ownerIndexes) {
        int dataSize = ClientMessage.HEADER_SIZE;
        for (Address address : addresses) {
            dataSize += ParameterUtil.calculateAddressDataSize(address);
        }
        dataSize += ownerIndexes.length * BitUtil.SIZE_OF_INT;
        return dataSize;
    }
}
