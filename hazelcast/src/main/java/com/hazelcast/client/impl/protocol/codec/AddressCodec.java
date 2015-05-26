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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.net.UnknownHostException;

public final class AddressCodec {

    private AddressCodec() {
    }

    public static Address decode(ClientMessage clientMessage) {
        boolean isNull = clientMessage.getBoolean();
        if (isNull) {
            return null;
        }
        String host = clientMessage.getStringUtf8();
        int port = clientMessage.getInt();
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public static void encode(Address address, ClientMessage clientMessage) {
        boolean isNull = address == null;
        clientMessage.set(isNull);
        if (isNull) {
            return;
        }
        clientMessage.set(address.getHost()).set(address.getPort());
    }

    public static int calculateDataSize(Address address) {
        boolean isNull = address == null;
        int dataSize = Bits.BOOLEAN_SIZE_IN_BYTES;
        if (isNull) {
            return dataSize;
        }
        dataSize += ParameterUtil.calculateStringDataSize(address.getHost());
        dataSize += Bits.INT_SIZE_IN_BYTES;
        return dataSize;
    }
}
