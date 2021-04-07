/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.hadoop.file.model;

import java.io.Serializable;
import java.util.Objects;

public class IncorrectUser implements Serializable {

    private String surname;
    private int age;

    public IncorrectUser() {
    }

    public IncorrectUser(String surname, int age) {
        this.surname = surname;
        this.age = age;
    }

    public String getSurname() {
        return surname;
    }

    public int getAge() {
        return age;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IncorrectUser user = (IncorrectUser) o;
        return age == user.age
                && Objects.equals(surname, user.surname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(surname, age);
    }
}
