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

package com.hazelcast.jet.io.tuple;

import com.hazelcast.jet.io.JetIoException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class Tuple2<T0, T1> implements Tuple {
    protected T0 c0;
    protected T1 c1;

    public Tuple2() {
    }

    public Tuple2(T0 c0, T1 c1) {
        this.c0 = c0;
        this.c1 = c1;
    }

    public T0 get0() {
        return c0;
    }

    public T1 get1() {
        return c1;
    }

    public void set0(T0 c0) {
        this.c0 = c0;
    }

    public void set1(T1 c1) {
        this.c1 = c1;
    }

    @Override
    public <T> T get(int index) {
        final Object result = index == 0 ? c0
                            : index == 1 ? c1
                            : error("Attempt to access component at index " + index);
        return (T) result;
    }

    @Override
    public void set(int index, Object value) {
        if (index == 0) {
            this.c0 = (T0) value;
        } else if (index == 1) {
            this.c1 = (T1) value;
        } else {
            error("Attempt to set a component at index " + index);
        }
    }

    @Override
    public int size() {
        return 2;
    }

    @Override
    public Object[] toArray() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(c0);
        out.writeObject(c1);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        c0 = in.readObject();
        c1 = in.readObject();
    }

    private static Object error(String msg) {
        throw new JetIoException(msg);
    }
}
