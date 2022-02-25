/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DummyOperation extends Operation {
    public Object value;
    public Object result;
    private int delayMillis;

    public DummyOperation() {
    }

    public DummyOperation(Object value) {
        this.value = value;
    }

    public DummyOperation setDelayMillis(int delayMillis) {
        this.delayMillis = delayMillis;
        return this;
    }

    @Override
    public void run() throws Exception {
        if (delayMillis > 0) {
            Thread.sleep(delayMillis);
        }

        if (value instanceof Runnable) {
            ((Runnable) value).run();
            result = value;
        } else if (value instanceof Callable) {
            result = ((Callable) value).call();
        } else {
            result = value;
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(value);
        out.writeInt(delayMillis);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readObject();
        delayMillis = in.readInt();
    }
}
