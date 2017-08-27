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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.util.Properties;

@Codec(RingbufferStoreConfigHolder.class)
@Since("1.5")
public final class RingbufferStoreConfigCodec {

    private static final byte CONFIG_TYPE_CLASS_NAME = 0;
    private static final byte CONFIG_TYPE_FACTORY_CLASS_NAME = 1;
    private static final byte CONFIG_TYPE_STORE_IMPLEMENTATION = 2;
    private static final byte CONFIG_TYPE_FACTORY_IMPLEMENTATION = 3;

    private RingbufferStoreConfigCodec() {
    }

    public static RingbufferStoreConfigHolder decode(ClientMessage clientMessage) {
        // one of className, factoryClassName, implementation, factoryImplement can be non-null
        byte storeConfigType = clientMessage.getByte();
        String className = null;
        String factoryClassName = null;
        Data implementation = null;
        Data factoryImplementation = null;
        switch (storeConfigType) {
            case CONFIG_TYPE_CLASS_NAME:
                className = clientMessage.getStringUtf8();
                break;
            case CONFIG_TYPE_FACTORY_CLASS_NAME:
                factoryClassName = clientMessage.getStringUtf8();
                break;
            case CONFIG_TYPE_STORE_IMPLEMENTATION:
                implementation = clientMessage.getData();
                break;
            case CONFIG_TYPE_FACTORY_IMPLEMENTATION:
                factoryImplementation = clientMessage.getData();
                break;
            default:
                throw new HazelcastSerializationException(String.format("Cannot decode ringbuffer store type %d",
                        storeConfigType));
        }
        boolean isNullProperties = clientMessage.getBoolean();
        Properties properties = null;
        if (!isNullProperties) {
            properties = PropertiesCodec.decode(clientMessage);
        }
        boolean enabled = clientMessage.getBoolean();
        return new RingbufferStoreConfigHolder(className, factoryClassName, implementation, factoryImplementation,
                properties, enabled);
    }

    public static void encode(RingbufferStoreConfigHolder storeConfig, ClientMessage clientMessage) {
        if (storeConfig.getImplementation() != null) {
            clientMessage.set(CONFIG_TYPE_STORE_IMPLEMENTATION)
                         .set(storeConfig.getImplementation());
        } else if (storeConfig.getClassName() != null) {
            clientMessage.set(CONFIG_TYPE_CLASS_NAME)
                         .set(storeConfig.getClassName());
        } else if (storeConfig.getFactoryImplementation() != null) {
            clientMessage.set(CONFIG_TYPE_FACTORY_IMPLEMENTATION)
                         .set(storeConfig.getFactoryImplementation());
        } else {
            clientMessage.set(CONFIG_TYPE_FACTORY_CLASS_NAME)
                         .set(storeConfig.getFactoryClassName());
        }
        boolean isNullProperties = storeConfig.getProperties() == null;
        clientMessage.set(isNullProperties);
        if (!isNullProperties) {
            PropertiesCodec.encode(storeConfig.getProperties(), clientMessage);
        }
        clientMessage.set(storeConfig.isEnabled());
    }

    public static int calculateDataSize(RingbufferStoreConfigHolder storeConfig) {
        int dataSize = Bits.BYTE_SIZE_IN_BYTES + 2 * Bits.BOOLEAN_SIZE_IN_BYTES;
        if (storeConfig.getImplementation() != null) {
            dataSize += ParameterUtil.calculateDataSize(storeConfig.getImplementation());
        } else if (storeConfig.getClassName() != null) {
            dataSize += ParameterUtil.calculateDataSize(storeConfig.getClassName());
        } else if (storeConfig.getFactoryImplementation() != null) {
            dataSize += ParameterUtil.calculateDataSize(storeConfig.getFactoryImplementation());
        } else {
            dataSize += ParameterUtil.calculateDataSize(storeConfig.getFactoryClassName());
        }
        dataSize += PropertiesCodec.calculateDataSize(storeConfig.getProperties());

        return dataSize;
    }
}
