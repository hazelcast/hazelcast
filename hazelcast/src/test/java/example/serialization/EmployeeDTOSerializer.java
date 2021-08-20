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

package example.serialization;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.io.IOException;

public class EmployeeDTOSerializer implements CompactSerializer<EmployeeDTO> {
    @Nonnull
    @Override
    public EmployeeDTO read(@Nonnull CompactReader in) throws IOException {
        return new EmployeeDTO(in.readInt("age"), in.readLong("id"));
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) throws IOException {
        out.writeInt("age", object.getAge());
        out.writeLong("id", object.getId());
    }
}
