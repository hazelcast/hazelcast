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

class BoolArrayDTOSerializer implements CompactSerializer<BoolArrayDTO> {
    @Nonnull
    @Override
    public BoolArrayDTO read(@Nonnull CompactReader in) {
        boolean[] bools = in.readArrayOfBoolean("bools");
        return new BoolArrayDTO(bools);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull BoolArrayDTO object) {
        out.writeArrayOfBoolean("bools", object.bools);
    }
}

class BoolArrayDTOSerializer2 implements CompactSerializer<BoolArrayDTO> {
    int itemCount;

    BoolArrayDTOSerializer2(int itemCount) {
        this.itemCount = itemCount;
    }

    @Nonnull
    @Override
    public BoolArrayDTO read(@Nonnull CompactReader in) {
        boolean[] bools = new boolean[itemCount];
        for (int i = 0; i < itemCount; i++) {
            bools[i] = in.readBoolean(Integer.toString(i));
        }
        return new BoolArrayDTO(bools);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull BoolArrayDTO object) {
        for (int i = 0; i < itemCount; i++) {
            out.writeBoolean(Integer.toString(i), object.bools[i]);
        }
    }
}
