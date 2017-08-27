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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

@Codec(PredicateConfigHolder.class)
@Since("1.5")
public final class PredicateConfigCodec {

    private static final int ENCODED_BOOLEANS = 3;

    private PredicateConfigCodec() {
    }

    public static PredicateConfigHolder decode(ClientMessage clientMessage) {
        String className = null;
        String sql = null;
        Data implementation = null;
        boolean isNullClassName = clientMessage.getBoolean();
        if (!isNullClassName) {
            className = clientMessage.getStringUtf8();
        }
        boolean isNullSql = clientMessage.getBoolean();
        if (!isNullSql) {
            sql = clientMessage.getStringUtf8();
        }
        boolean isNullImplementation = clientMessage.getBoolean();
        if (!isNullImplementation) {
            implementation = clientMessage.getData();
        }
        return new PredicateConfigHolder(className, sql, implementation);
    }

    public static void encode(PredicateConfigHolder config, ClientMessage clientMessage) {
        boolean isNullClassName = config.getClassName() == null;
        boolean isNullSql = config.getSql() == null;
        boolean isNullImplementation = config.getImplementation() == null;
        clientMessage.set(isNullClassName);
        if (!isNullClassName) {
            clientMessage.set(config.getClassName());
        }
        clientMessage.set(isNullSql);
        if (!isNullSql) {
            clientMessage.set(config.getSql());
        }
        clientMessage.set(isNullImplementation);
        if (!isNullImplementation) {
            clientMessage.set(config.getImplementation());
        }
    }

    public static int calculateDataSize(PredicateConfigHolder config) {
        boolean hasClassName = config.getClassName() != null;
        boolean hasSql = config.getSql() != null;
        boolean hasImplementation = config.getImplementation() != null;
        int dataSize = ENCODED_BOOLEANS * Bits.BOOLEAN_SIZE_IN_BYTES;
        if (hasClassName) {
            dataSize += ParameterUtil.calculateDataSize(config.getClassName());
        }
        if (hasSql) {
            dataSize += ParameterUtil.calculateDataSize(config.getSql());
        }
        if (hasImplementation) {
            dataSize += ParameterUtil.calculateDataSize(config.getImplementation());
        }
        return dataSize;
    }
}
