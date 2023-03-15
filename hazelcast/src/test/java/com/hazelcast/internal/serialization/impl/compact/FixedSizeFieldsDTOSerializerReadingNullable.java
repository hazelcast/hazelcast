/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

public class FixedSizeFieldsDTOSerializerReadingNullable implements CompactSerializer<FixedSizeFieldsDTO> {
    @Nonnull
    @Override
    public FixedSizeFieldsDTO read(@Nonnull CompactReader reader) {
        Byte b = reader.readNullableInt8("b");
        Boolean bool = reader.readNullableBoolean("bool");
        Short s = reader.readNullableInt16("s");
        Integer i = reader.readNullableInt32("i");
        Long l = reader.readNullableInt64("l");
        Float f = reader.readNullableFloat32("f");
        Double d = reader.readNullableFloat64("d");
        return new FixedSizeFieldsDTO(b, bool, s, i, l, f, d);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull FixedSizeFieldsDTO object) {
        out.writeInt8("b", object.b);
        out.writeBoolean("bool", object.bool);
        out.writeInt16("s", object.s);
        out.writeInt32("i", object.i);
        out.writeInt64("l", object.l);
        out.writeFloat32("f", object.f);
        out.writeFloat64("d", object.d);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "fixedSizeFieldsDTO";
    }

    @Nonnull
    @Override
    public Class<FixedSizeFieldsDTO> getCompactClass() {
        return FixedSizeFieldsDTO.class;
    }
}
