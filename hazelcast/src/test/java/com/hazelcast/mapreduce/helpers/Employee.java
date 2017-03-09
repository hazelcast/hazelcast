/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.helpers;

import java.io.Serializable;
import java.util.Random;

public class Employee implements Serializable, Comparable<Employee> {

    public static final int MAX_AGE = 75;
    public static final double MAX_SALARY = 1000.0;

    public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    public static Random random = new Random();

    private int id;
    private String name;
    private int age;
    private boolean active;
    private double salary;

    public Employee(String name, int age, boolean live, double salary) {
        this.name = name;
        this.age = age;
        this.active = live;
        this.salary = salary;
    }

    public Employee(int id) {
        this.id = id;
        setInfo();
    }

    public Employee() {
    }

    public void setInfo() {
        name = names[random.nextInt(names.length)];
        age = random.nextInt(MAX_AGE);
        active = random.nextBoolean();
        salary = random.nextDouble() * MAX_SALARY;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public double getSalary() {
        return salary;
    }

    public boolean isActive() {
        return active;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", active=" + active +
                ", salary=" + salary +
                '}';
    }

    @Override
    public int compareTo(Employee employee) {
        return id - employee.id;
    }
}
