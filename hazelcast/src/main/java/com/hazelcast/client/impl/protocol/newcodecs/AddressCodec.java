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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.net.UnknownHostException;
import java.util.Iterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;

class AddressCodec {

    private static final int PORT_OFFSET = 0;
    private static final int HEADER_SIZE = PORT_OFFSET + Bits.INT_SIZE_IN_BYTES;

    static void encode(ClientMessage clientMessage, Address address) {
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], DEFAULT_FLAGS);
        Bits.writeIntL(initialFrame.content, 0, address.getPort());

        clientMessage.addFrame(initialFrame);
        clientMessage.addFrame(new ClientMessage.Frame(address.getHost().getBytes(Bits.UTF_8), DEFAULT_FLAGS));
    }

    static Address decode(Iterator<ClientMessage.Frame> iterator) {
        ClientMessage.Frame initialFrame = iterator.next();

        int port = Bits.readIntL(initialFrame.content, 0);
        String host = new String(iterator.next().content, Bits.UTF_8);

        Address address;
        try {
            address = new Address(host, port);
        } catch (UnknownHostException exception) {
            throw new HazelcastException(exception);
        }
        return address;
    }
}
