/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.test.executor.tasks;

import com.hazelcast.client.test.IdentifiedFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * This class is for Non-java clients as well. Please do not remove or modify.
 */
public class CancellationAwareTask
        implements Callable<Boolean>, IdentifiedDataSerializable {
    public static final int CLASS_ID = 6;

    private long sleepTime;

    public CancellationAwareTask() {
    }

    public CancellationAwareTask(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public Boolean call()
            throws InterruptedException {
        Thread.sleep(sleepTime);
        return Boolean.TRUE;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(sleepTime);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        sleepTime = in.readLong();
    }
}
