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

package com.hazelcast.console;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Echoes to screen.
 */
@BinaryInterface
public class Echo implements Callable<String>, DataSerializable, HazelcastInstanceAware {

    String input;

    private transient HazelcastInstance hz;

    public Echo() {
        // no-arg constructor needed for DataSerializable classes
    }

    public Echo(String input) {
        this.input = input;
    }

    @Override
    public String call() {
        hz.getCountDownLatch("latch").countDown();
        return hz.getCluster().getLocalMember().toString() + ":" + input;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(input);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        input = in.readUTF();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}
