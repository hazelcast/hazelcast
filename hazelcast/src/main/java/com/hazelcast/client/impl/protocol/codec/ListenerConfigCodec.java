/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.annotation.Codec;
import com.hazelcast.annotation.Since;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

@Codec(ListenerConfigHolder.class)
@Since("1.5")
public final class ListenerConfigCodec {

    private ListenerConfigCodec() {
    }

    public static ListenerConfigHolder decode(ClientMessage clientMessage) {
        byte listenerType = clientMessage.getByte();
        boolean isNullListenerImplementation = clientMessage.getBoolean();
        Data implementation = null;
        String className = null;
        if (!isNullListenerImplementation) {
            implementation = clientMessage.getData();
        }
        boolean isNullClassName = clientMessage.getBoolean();
        if (!isNullClassName) {
            className = clientMessage.getStringUtf8();
        }
        boolean local = clientMessage.getBoolean();
        boolean includeValue = clientMessage.getBoolean();
        if (className == null) {
            return new ListenerConfigHolder(listenerType, implementation, includeValue, local);
        } else {
            return new ListenerConfigHolder(listenerType, className, includeValue, local);
        }

    }

    public static void encode(ListenerConfigHolder listenerConfigHolder, ClientMessage clientMessage) {
        clientMessage.set((byte) listenerConfigHolder.getListenerType());
        boolean isNullImplementation = listenerConfigHolder.getListenerImplementation() == null;
        clientMessage.set(isNullImplementation);
        if (!isNullImplementation) {
            clientMessage.set(listenerConfigHolder.getListenerImplementation());
        }
        boolean isNullClassName = listenerConfigHolder.getClassName() == null;
        clientMessage.set(isNullClassName);
        if (!isNullClassName) {
            clientMessage.set(listenerConfigHolder.getClassName());
        }
        clientMessage.set(listenerConfigHolder.isLocal()).set(listenerConfigHolder.isIncludeValue());
    }

    public static int calculateDataSize(ListenerConfigHolder listenerConfig) {
        boolean hasImplementation = (listenerConfig.getListenerImplementation() != null);
        int dataSize = 3 * Bits.BOOLEAN_SIZE_IN_BYTES + Bits.BYTE_SIZE_IN_BYTES;
        if (hasImplementation) {
            dataSize += ParameterUtil.calculateDataSize(listenerConfig.getListenerImplementation());
        } else {
            dataSize += ParameterUtil.calculateDataSize(listenerConfig.getClassName());
        }
        return dataSize;
    }
}
