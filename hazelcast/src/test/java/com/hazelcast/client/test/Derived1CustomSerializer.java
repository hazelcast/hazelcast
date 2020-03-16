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

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */

package com.hazelcast.client.test;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class Derived1CustomSerializer implements StreamSerializer<Derived1Custom> {
    @Override
    public void write(ObjectDataOutput out, Derived1Custom object)
            throws IOException {
        out.writeInt(object.getValue());
    }

    @Override
    public Derived1Custom read(ObjectDataInput in)
            throws IOException {
        return new Derived1Custom(in.readInt());
    }

    @Override
    public int getTypeId() {
        return 4;
    }
}
