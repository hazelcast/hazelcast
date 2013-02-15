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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class CountDownLatchInfo implements DataSerializable {

    private String name;
    private int count = 0;
    private String owner;

    public CountDownLatchInfo() {
    }

    public CountDownLatchInfo(String name) {
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

    public String getOwner() {
        return owner;
    }

    public String getName() {
        return name;
    }

    public boolean setCount(int count, String owner) {
        if (this.count > 0 || count <= 0) {
            return false;
        }
        this.count = count;
        this.owner = owner;
        return true;
    }

    public void setCountDirect(int count, String owner) {
        this.count = count;
        this.owner = owner;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LocalCountDownLatch");
        sb.append("{name='").append(name).append('\'');
        sb.append(", count=").append(count);
        sb.append(", owner=").append(owner);
        sb.append('}');
        return sb.toString();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(count);
        out.writeUTF(owner);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        count = in.readInt();
        owner = in.readUTF();
    }
}
