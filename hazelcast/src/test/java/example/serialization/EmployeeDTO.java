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

package example.serialization;

import javax.annotation.Nonnull;
import java.util.Objects;

public class EmployeeDTO implements Comparable<EmployeeDTO> {

    private int age;
    private int rank;
    private long id;
    private boolean isHired;
    private boolean isFired;

    public EmployeeDTO() {
    }

    public EmployeeDTO(int age, long id) {
        this.age = age;
        this.id = id;
        this.isFired = false;
        this.isHired = true;
        this.rank = age;
    }

    @Override
    public String toString() {
        return "EmployeeDTO{"
                + "age=" + age
                + ", id=" + id
                + ", isHired=" + isHired
                + ", isFired=" + isFired
                + ", rank=" + rank
                + '}';
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EmployeeDTO that = (EmployeeDTO) o;
        return age == that.age && id == that.id && isHired == that.isHired && isFired == that.isFired
                && rank == that.rank;
    }

    @Override
    public int hashCode() {
        return Objects.hash(age, id, isHired, isFired, rank);
    }

    @Override
    public int compareTo(@Nonnull EmployeeDTO o) {
        return (int) (o.id - id);
    }


}
