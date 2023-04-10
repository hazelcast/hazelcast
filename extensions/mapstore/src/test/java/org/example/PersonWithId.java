/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import java.util.Objects;

/**
 * This class must be in other package than com.hazelcast.*
 */
public class PersonWithId {

    int personId;
    String name;

    public PersonWithId() {
    }

    public PersonWithId(int personId, String name) {
        this.personId = personId;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPersonId() {
        return personId;
    }

    public void setPersonId(int personId) {
        this.personId = personId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PersonWithId)) {
            return false;
        }
        PersonWithId that = (PersonWithId) o;
        return personId == that.personId && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(personId, name);
    }

    @Override
    public String toString() {
        return "PersonWithObjectId{" +
                ", personId=" + personId +
                ", name='" + name + '\'' +
                '}';
    }
}
