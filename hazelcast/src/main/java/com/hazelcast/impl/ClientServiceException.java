/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.nio.AbstractSerializer;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClientServiceException implements DataSerializable {
    Throwable throwable;

    public ClientServiceException() {
    }

    public ClientServiceException(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void writeData(DataOutput out) throws IOException {
        boolean isDs = throwable instanceof DataSerializable;
        out.writeBoolean(isDs);
        if (isDs) {
            out.writeUTF(throwable.getClass().getName());
            ((DataSerializable) throwable).writeData(out);
        } else
            out.writeUTF(throwable.getMessage());
    }

    public void readData(DataInput in) throws IOException {
        boolean isDs = in.readBoolean();
        if (isDs) {
            String className = in.readUTF();
            final DataSerializable ds;
            try {
                ds = (DataSerializable) AbstractSerializer.newInstance(AbstractSerializer.loadClass(className));
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
            ds.readData(in);
            throwable = (Throwable) ds;
        } else {
            throwable = new RuntimeException(in.readUTF());
        }
    }
}
