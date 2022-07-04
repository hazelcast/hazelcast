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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.PortableReader;

import java.io.IOException;

class InvalidRawDataPortable2 extends RawDataPortable {

    InvalidRawDataPortable2() {
    }

    InvalidRawDataPortable2(long l, char[] c, NamedPortable p, int k, String s, ByteArrayDataSerializable sds) {
        super(l, c, p, k, s, sds);
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.INVALID_RAW_DATA_PORTABLE_2;
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        c = reader.readCharArray("c");
        final ObjectDataInput input = reader.getRawDataInput();
        k = input.readInt();
        l = reader.readLong("l");
        s = input.readString();
        p = reader.readPortable("p");
        sds = input.readObject();
    }
}
