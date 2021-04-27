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

public class EmployeeDTO {

    private int age;
    private long id;

    public EmployeeDTO() {
    }

    public EmployeeDTO(int age, long id) {
        this.age = age;
        this.id = id;
    }

    @Override
    public String toString() {
        return "EmployeeDTO{"
                + "age=" + age + ", id=" + id
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

        EmployeeDTO that = (EmployeeDTO) o;

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
}
