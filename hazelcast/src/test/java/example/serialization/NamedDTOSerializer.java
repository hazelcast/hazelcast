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

package example.serialization;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class NamedDTOSerializer implements CompactSerializer<NamedDTO> {
    @Nonnull
    @Override
    public NamedDTO read(@Nonnull CompactReader reader) {
        String name = reader.getFieldKind("name") == FieldKind.STRING
                ? reader.readString("name")
                : "";

        int myint = reader.getFieldKind("myint") == FieldKind.INT32
                ? reader.readInt32("myint")
                : 0;
        return new NamedDTO(name, myint);
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull NamedDTO object) {
        writer.writeString("name", object.name);
        writer.writeInt32("myint", object.myint);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "namedDTO";
    }

    @Nonnull
    @Override
    public Class<NamedDTO> getCompactClass() {
        return NamedDTO.class;
    }
}
