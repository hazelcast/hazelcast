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

package com.hazelcast.map.merge;

import java.io.Serializable;
import java.util.Random;

public class MyPerson implements Serializable {

    private static Random random = new Random();

    long personId;

    long age;

    String firstName;

    String lastName;

    double salary;

    long count = 0;

    public MyPerson() {

    }

    public MyPerson(long personId) {
        this.personId = personId;
        this.age = personId;
        this.firstName = "" + personId;
        this.lastName = "" + personId;
        this.salary = random.nextDouble();
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || personId == ((MyPerson) o).getPersonId();
    }

    @Override
    public int hashCode() {
        return (int) personId;
    }

    @Override
    public String toString() {
        return "MyPerson{"
                + "personId=" + personId
                + ", age=" + age
                + ", firstName='" + firstName + '\''
                + ", lastName='" + lastName + '\''
                + ", salary=" + salary
                + ", count=" + count
                + '}';
    }
}
