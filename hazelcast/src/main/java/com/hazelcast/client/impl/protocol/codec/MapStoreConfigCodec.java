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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

@Codec(MapStoreConfigHolder.class)
@Since("1.5")
public final class MapStoreConfigCodec {

    private static final int ENCODED_BOOLEANS = 7;
    private static final int ENCODED_INTS = 2;

    private MapStoreConfigCodec() {
    }

    public static MapStoreConfigHolder decode(ClientMessage clientMessage) {
        MapStoreConfigHolder config = new MapStoreConfigHolder();
        config.setEnabled(clientMessage.getBoolean());
        config.setWriteCoalescing(clientMessage.getBoolean());
        config.setWriteDelaySeconds(clientMessage.getInt());
        config.setWriteBatchSize(clientMessage.getInt());
        config.setInitialLoadMode(clientMessage.getStringUtf8());
        boolean isNullProperties = clientMessage.getBoolean();
        if (!isNullProperties) {
            config.setProperties(PropertiesCodec.decode(clientMessage));
        }
        boolean isNullClassName = clientMessage.getBoolean();
        if (!isNullClassName) {
            config.setClassName(clientMessage.getStringUtf8());
        }
        boolean isNullFactoryClassName = clientMessage.getBoolean();
        if (!isNullFactoryClassName) {
            config.setFactoryClassName(clientMessage.getStringUtf8());
        }
        boolean isNullImplementation = clientMessage.getBoolean();
        if (!isNullImplementation) {
            config.setImplementation(clientMessage.getData());
        }
        boolean isNullFactoryImplementation = clientMessage.getBoolean();
        if (!isNullFactoryImplementation) {
            config.setFactoryImplementation(clientMessage.getData());
        }
        return config;
    }

    public static void encode(MapStoreConfigHolder config, ClientMessage clientMessage) {
        clientMessage.set(config.isEnabled()).set(config.isWriteCoalescing())
                     .set(config.getWriteDelaySeconds()).set(config.getWriteBatchSize())
                     .set(config.getInitialLoadMode());
        boolean isNullProperties = config.getProperties() == null;
        clientMessage.set(isNullProperties);
        if (!isNullProperties) {
            PropertiesCodec.encode(config.getProperties(), clientMessage);
        }
        boolean isNullClassName = config.getClassName() == null;
        clientMessage.set(isNullClassName);
        if (!isNullClassName) {
            clientMessage.set(config.getClassName());
        }
        boolean isNullFactoryClassName = config.getFactoryClassName() == null;
        clientMessage.set(isNullFactoryClassName);
        if (!isNullFactoryClassName) {
            clientMessage.set(config.getFactoryClassName());
        }
        boolean isNullImplementation = config.getImplementation() == null;
        clientMessage.set(isNullImplementation);
        if (!isNullImplementation) {
            clientMessage.set(config.getImplementation());
        }
        boolean isNullFactoryImplementation = config.getFactoryImplementation() == null;
        clientMessage.set(isNullFactoryImplementation);
        if (!isNullFactoryImplementation) {
            clientMessage.set(config.getFactoryImplementation());
        }
    }

    public static int calculateDataSize(MapStoreConfigHolder config) {
        int dataSize = ENCODED_BOOLEANS * BOOLEAN_SIZE_IN_BYTES + ENCODED_INTS * INT_SIZE_IN_BYTES;
        if (config.getProperties() != null) {
            dataSize += PropertiesCodec.calculateDataSize(config.getProperties());
        }
        dataSize += ParameterUtil.calculateDataSize(config.getInitialLoadMode());
        if (config.getClassName() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getClassName());
        }
        if (config.getFactoryClassName() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getFactoryClassName());
        }
        if (config.getImplementation() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getImplementation());
        }
        if (config.getFactoryImplementation() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getFactoryImplementation());
        }
        return dataSize;
    }
}
