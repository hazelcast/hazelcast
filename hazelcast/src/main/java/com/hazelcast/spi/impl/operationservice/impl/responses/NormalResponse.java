/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;

/**
 * A NormalResponse is send when an Operation needs to return a value. This response value can a 'normal' value,
 * but it can also contain the exception thrown.
 * <p/>
 * Currently there is a limitation in the design that needs to be dealt with in the future: there is no distinction
 * made between an exception thrown or an exception returned as a regular value. In such a case, Hazelcast will
 * always rethrow the exception.
 * <p/>
 * The NormalResponse contains the actual 'value' but also the callid of that operation
 * and the backup count. Based on the backup count, the invoker of the operation
 * knows when all the backups have completed.
 *
 * @author mdogan 4/10/13
 */
public class NormalResponse extends Response {
    public static final int OFFSET_BACKUP_ACKS = RESPONSE_SIZE_IN_BYTES;
    public static final int OFFSET_IS_DATA = OFFSET_BACKUP_ACKS + 1;
    public static final int OFFSET_NOT_DATA = OFFSET_IS_DATA + 1;
    public static final int OFFSET_DATA_LENGTH = OFFSET_IS_DATA + 1;
    public static final int OFFSET_DATA_PAYLOAD = OFFSET_DATA_LENGTH + INT_SIZE_IN_BYTES;

    private Object value;

    //the number of backups acks; 0 if no acks are needed.
    private int backupAcks;

    public NormalResponse() {
    }

    public NormalResponse(Object value, long callId, int backupAcks, boolean urgent) {
        super(callId, urgent);
        this.value = value;
        this.backupAcks = backupAcks;
    }

    /**
     * Returns the object value of the operation.
     *
     * @return The object value.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Returns the number of backups that needs to acknowledge before the invocation completes.
     *
     * @return The number of backup acknowledgements backups.
     */
    public int getBackupAcks() {
        return backupAcks;
    }

    @Override
    public int getId() {
        return NORMAL_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        // acks fit in a byte.
        out.writeByte(backupAcks);

        final boolean isData = value instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            out.writeData((Data) value);
        } else {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        backupAcks = in.readByte();

        final boolean isData = in.readBoolean();
        if (isData) {
            value = in.readData();
        } else {
            value = in.readObject();
        }
    }

    @Override
    public String toString() {
        return "NormalResponse{"
                + "callId=" + callId
                + ", urgent=" + urgent
                + ", value=" + value
                + ", backupAcks=" + backupAcks
                + '}';
    }
}
