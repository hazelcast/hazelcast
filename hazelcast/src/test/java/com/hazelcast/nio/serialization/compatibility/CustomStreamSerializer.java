/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class CustomStreamSerializer implements StreamSerializer<CustomStreamSerializable> {

    @Override
    public void write(ObjectDataOutput out, CustomStreamSerializable object) throws IOException {
        out.writeInt(object.i);
        out.writeFloat(object.f);
    }

    @Override
    public CustomStreamSerializable read(ObjectDataInput in) throws IOException {
        return new CustomStreamSerializable(in.readInt(), in.readFloat());
    }

    @Override
    public int getTypeId() {
        return ReferenceObjects.CUSTOM_STREAM_SERIALIZABLE_ID;
    }

    @Override
    public void destroy() {
    }
}
