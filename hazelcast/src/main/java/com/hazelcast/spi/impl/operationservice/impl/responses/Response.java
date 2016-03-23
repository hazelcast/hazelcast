/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.ERROR_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;

/**
 * A {@link Response} is a result of an {@link com.hazelcast.spi.Operation} being executed.
 * There are different types of responses:
 * <ol>
 * <li>
 * {@link NormalResponse} the result of a regular Operation result, e.g. Map.put
 * </li>
 * <li>
 * {@link BackupResponse} the result of a completed {@link com.hazelcast.spi.impl.operationservice.impl.operations.Backup}.
 * </li>
 * </ol>
 */
//todo: we need to fix the magic numbers
@SuppressWarnings("checkstyle:magicnumber")
public abstract class Response implements IdentifiedDataSerializable {
    public static final boolean USE_BIG_ENDIAN = true;


    protected long callId;
    protected boolean urgent;

    public Response() {
    }

    public Response(long callId, boolean urgent) {
        this.callId = callId;
        this.urgent = urgent;
    }

    /**
     * Check if this Response is an urgent response.
     *
     * @return true if urgent, false otherwise.
     */
    public boolean isUrgent() {
        return urgent;
    }

    /**
     * Returns the call id of the operation this response belongs to.
     *
     * @return the call id.
     */
    public long getCallId() {
        return callId;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(callId);
        out.writeBoolean(urgent);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readLong();
        urgent = in.readBoolean();
    }


    public static long callId(byte[] bytes) {
        //int:partition hash
        //int:serializer type id
        //byte:identified
        //int:factory-id
        //int: type id

        return Bits.readLong(bytes, 17, USE_BIG_ENDIAN);
    }

    public static int typeId(byte[] bytes) {
        return Bits.readInt(bytes, 13, USE_BIG_ENDIAN);
    }

    public static int backupCount(byte[] bytes) {
        return Bits.readInt(bytes, 26, USE_BIG_ENDIAN);
    }

    public static int serializerId(byte[] bytes) {
        return Bits.readInt(bytes, 4, USE_BIG_ENDIAN);
    }

    public static int factoryId(byte[] bytes) {
        return Bits.readInt(bytes, 9, USE_BIG_ENDIAN);
    }

    public static Object deserializeValue(InternalSerializationService serializationService, Object data) {
        if (data instanceof NormalResponse) {
            data = ((NormalResponse) data).getValue();
        }

        if (!(data instanceof Data)) {
            return data;
        }

        return deserializeValue(serializationService, (Data) data);
    }

    public static Object deserializeValue(InternalSerializationService serializationService, Data data) {
        byte[] bytes = data.toByteArray();

        int offset = HeapData.TYPE_OFFSET;

        if (serializerId(bytes) == CONSTANT_TYPE_DATA_SERIALIZABLE && factoryId(bytes) == SpiDataSerializerHook.F_ID) {
            switch (typeId(bytes)) {
                case NORMAL_RESPONSE:
                    byte isData = bytes[30];
                    if (isData == 1) {
                        return serializationService.bytesToObject(bytes, 39);
                    }
                    offset = 31;
                    break;
                case ERROR_RESPONSE:
                    offset = 26;
                    break;
                default:
                    //no-op
                    break;
            }
        }

        return serializationService.bytesToObject(bytes, offset);
    }

    public static Object getValueAsData(InternalSerializationService serializationService, Data data) {
        byte[] bytes = data.toByteArray();

        boolean normalResponse = serializerId(bytes) == CONSTANT_TYPE_DATA_SERIALIZABLE
                && factoryId(bytes) == SpiDataSerializerHook.F_ID
                && typeId(bytes) == NORMAL_RESPONSE;

        if (!normalResponse) {
            return data;
        }

        byte isData = bytes[30];
        if (isData == 1) {
            int size = Bits.readIntB(bytes, 31);
            byte[] valueBytes = new byte[size];
            System.arraycopy(bytes, 35, valueBytes, 0, valueBytes.length);
            return new HeapData(valueBytes);
        } else {
            return serializationService.bytesToObject(bytes, 31);
        }
    }
}
