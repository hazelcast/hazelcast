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

import com.hazelcast.internal.serialization.impl.compact.Compactable;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.io.IOException;

/**
 * An example class which is planned to be generated via code generator
 */
public class EmployeeWithSerializerDTO implements Compactable<EmployeeWithSerializerDTO> {

    private static final CompactSerializer<EmployeeWithSerializerDTO> SERIALIZER =
            new CompactSerializer<EmployeeWithSerializerDTO>() {
                @Override
                public EmployeeWithSerializerDTO read(CompactReader in) throws IOException {
                    EmployeeWithSerializerDTO employee = new EmployeeWithSerializerDTO();
                    employee.age = in.readInt("a");
                    employee.id = in.readLong("i");
                    return employee;
                }

                @Override
                public void write(CompactWriter out, EmployeeWithSerializerDTO object) throws IOException {
                    out.writeInt("a", object.age);
                    out.writeLong("i", object.id);
                }
            };

    private int age;
    private long id;

    public EmployeeWithSerializerDTO() {
    }

    public EmployeeWithSerializerDTO(int age, long id) {
        this.age = age;
        this.id = id;
    }

    @Override
    public String toString() {
        return "EmployeeDTO{"
                + "age=" + age
                + ", id=" + id
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmployeeWithSerializerDTO that = (EmployeeWithSerializerDTO) o;

        if (age != that.age) {
            return false;
        }
        return id == that.id;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        int result = age;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }


    @Override
    public CompactSerializer<EmployeeWithSerializerDTO> getCompactSerializer() {
        return SERIALIZER;
    }
}
