/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */

package com.hazelcast.client.test.ringbuffer.filter;

import com.hazelcast.client.test.IdentifiedDataSerializableFactory;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class StartsWithStringFilter implements IFunction<String, Boolean>, IdentifiedDataSerializable {
    public static final int CLASS_ID = 14;

    private String startString;

    public StartsWithStringFilter(String startString) {
        this.startString = startString;
    }

    public StartsWithStringFilter() {
    }

    @Override
    public Boolean apply(String input) {
        return input.startsWith(startString);
    }

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(startString);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        startString = in.readUTF();
    }
}
