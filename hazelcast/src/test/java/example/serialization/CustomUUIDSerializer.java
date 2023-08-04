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
import java.util.Objects;
import java.util.UUID;

public class CustomUUIDSerializer implements CompactSerializer<UUID> {

    @Nonnull
    @Override
    public UUID read(@Nonnull CompactReader reader) {
        return UUID.fromString(Objects.requireNonNull(reader.readString("uuidField")));
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull UUID object) {
        writer.writeString("uuidField", object.toString());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "myUUID";
    }

    @Nonnull
    @Override
    public Class<UUID> getCompactClass() {
        return UUID.class;
    }

}
