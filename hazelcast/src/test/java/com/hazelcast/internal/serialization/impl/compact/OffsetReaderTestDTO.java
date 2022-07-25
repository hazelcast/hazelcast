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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;

class OffsetReaderTestDTO {
    public String[] arrayOfStr;
    public int i;
    public String str;

    OffsetReaderTestDTO(String[] arrayOfStr, int i, String str) {
        this.arrayOfStr = arrayOfStr;
        this.i = i;
        this.str = str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetReaderTestDTO a = (OffsetReaderTestDTO) o;
        return i == a.i && Arrays.equals(arrayOfStr, a.arrayOfStr) && Objects.equals(str, a.str);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(i, str);
        result = 31 * result + Arrays.hashCode(arrayOfStr);
        return result;
    }
}

class OffsetReaderTestDTOSerializer implements CompactSerializer<OffsetReaderTestDTO> {
    @Nonnull
    @Override
    public OffsetReaderTestDTO read(@Nonnull CompactReader in) {
        String[] arrayOfStr = in.readArrayOfString("arrayOfStr");
        int i = in.readInt32("i");
        String str = in.readString("str");
        return new OffsetReaderTestDTO(arrayOfStr, i, str);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull OffsetReaderTestDTO object) {
        out.writeArrayOfString("arrayOfStr", object.arrayOfStr);
        out.writeInt32("i", object.i);
        out.writeString("str", object.str);
    }
}
