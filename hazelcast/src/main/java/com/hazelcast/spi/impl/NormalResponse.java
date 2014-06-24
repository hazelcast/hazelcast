/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

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

    private Object value;

    //the number of synchronous backups; 0 if no backups are needed.
    private int backupCount;

    public NormalResponse() {
    }

    public NormalResponse(Object value, long callId, int backupCount, boolean urgent) {
        super(callId, urgent);
        this.value = value;
        this.backupCount = backupCount;
    }

    public Object getValue() {
        return value;
    }

    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.NORMAL_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(backupCount);

        final boolean isData = value instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            ((Data) value).writeData(out);
        } else {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        backupCount = in.readInt();

        final boolean isData = in.readBoolean();
        if (isData) {
            Data data = new Data();
            data.readData(in);
            value = data;
        } else {
            value = in.readObject();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NormalResponse");
        sb.append("{callId=").append(callId);
        sb.append(", urgent=").append(urgent);
        sb.append(", value=").append(value);
        sb.append(", backupCount=").append(backupCount);
        sb.append('}');
        return sb.toString();
    }
}
