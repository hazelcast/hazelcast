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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.net.UnknownHostException;
import java.util.Collection;

public class ParameterUtil {

    public static int calculateStringDataSize(String string) {
        if (string == null) {
            return Bits.INT_SIZE_IN_BYTES * 5;
        }
        return Bits.INT_SIZE_IN_BYTES + string.length() * 4;
    }

    public static int calculateByteArrayDataSize(byte[] bytes) {
        if (bytes == null) {
            return Bits.INT_SIZE_IN_BYTES;
        }
        return Bits.INT_SIZE_IN_BYTES + bytes.length;
    }

    public static int calculateIntArrayDataSize(int[] intArray) {
        if (intArray == null) {
            return Bits.INT_SIZE_IN_BYTES;
        }
        return Bits.INT_SIZE_IN_BYTES + intArray.length * Bits.INT_SIZE_IN_BYTES;
    }


    public static int calculateAddressDataSize(Address address) {
        boolean isNull = address == null;
        if (isNull) {
            return Bits.BOOLEAN_SIZE_IN_BYTES;
        }
        int dataSize = calculateStringDataSize(address.getHost());
        dataSize += Bits.INT_SIZE_IN_BYTES;
        return dataSize;
    }

    public static void encodeAddress(ClientMessage clientMessage, Address address) {
        boolean isNull = address == null;
        clientMessage.set(isNull);
        if (isNull) {
            return;
        }
        clientMessage.set(address.getHost()).set(address.getPort());

    }

    public static Address decodeAddress(ClientMessage clientMessage) throws UnknownHostException {
        boolean isNull = clientMessage.getBoolean();
        if (isNull) {
            return null;
        }
        String host = clientMessage.getStringUtf8();
        int port = clientMessage.getInt();
        return new Address(host, port);

    }

    public static int calculateDataSize(Data key) {
        return calculateByteArrayDataSize(key.toByteArray());
    }

    public static int calculateCollectionDataSize(Collection<Data> dataCollection) {
        int total = Bits.INT_SIZE_IN_BYTES;
        for (Data data : dataCollection) {
            total += calculateDataSize(data);
        }
        return total;
    }
}
