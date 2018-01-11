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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.CONTAINER;
import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.F_ID;

public class CountDownLatchContainer implements IdentifiedDataSerializable {

    private String name;
    private int count;

    public CountDownLatchContainer() {
    }

    public CountDownLatchContainer(String name) {
        this.name = name;
    }

    public int countDown() {
        if (count > 0) {
            count--;
        }
        return count;
    }

    public int getCount() {
        return count;
    }

    public String getName() {
        return name;
    }

    public boolean setCount(int count) {
        if (this.count > 0 || count <= 0) {
            return false;
        }
        this.count = count;
        return true;
    }

    public void setCountDirect(int count) {
        this.count = count;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return "LocalCountDownLatch{name='" + name + '\'' + ", count=" + count + '}';
    }
}
