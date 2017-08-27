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
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;

import java.util.Properties;
import java.util.Set;

@Codec(Properties.class)
@Since("1.5")
public final class PropertiesCodec {

    private PropertiesCodec() {
    }

    public static Properties decode(ClientMessage clientMessage) {
        int entriesCount = clientMessage.getInt();
        Properties properties = new Properties();
        for (int i = 0; i < entriesCount; i++) {
            properties.setProperty(clientMessage.getStringUtf8(), clientMessage.getStringUtf8());
        }
        return properties;
    }

    public static void encode(Properties properties, ClientMessage clientMessage) {
        Set<String> propertyKeys = properties.stringPropertyNames();
        clientMessage.set(propertyKeys.size());
        for (String key : propertyKeys) {
            clientMessage.set(key).set(properties.getProperty(key));
        }
    }

    public static int calculateDataSize(Properties properties) {
        int dataSize = Bits.INT_SIZE_IN_BYTES;
        Set<String> propertyKeys = properties.stringPropertyNames();
        for (String key : propertyKeys) {
            dataSize += ParameterUtil.calculateDataSize(key);
            dataSize += ParameterUtil.calculateDataSize(properties.getProperty(key));
        }
        return dataSize;
    }

}
