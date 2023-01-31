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

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class SerializableEmployeeDTOSerializer implements CompactSerializer<SerializableEmployeeDTO> {
    @Nonnull
    @Override
    public SerializableEmployeeDTO read(@Nonnull CompactReader reader) {
        return new SerializableEmployeeDTO(reader.readString("name"), reader.readInt32("age"));
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull SerializableEmployeeDTO object) {
        writer.writeString("name", object.getName());
        writer.writeInt32("age", object.getAge());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return SerializableEmployeeDTO.class.getName();
    }

    @Nonnull
    @Override
    public Class<SerializableEmployeeDTO> getCompactClass() {
        return SerializableEmployeeDTO.class;
    }
}
