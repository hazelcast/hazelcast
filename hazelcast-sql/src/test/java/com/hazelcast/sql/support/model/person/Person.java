/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.support.model.person;

import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = -3664451191867982502L;

    private String name;
    private int age;
    private long salary;
    private long cityId;
    private String deptTitle;

    public Person() {
        // No-op.
    }

    public Person(String name, int age, long salary, long cityId, String deptTitle) {
        this.name = name;
        this.age = age;
        this.salary = salary;
        this.cityId = cityId;
        this.deptTitle = deptTitle;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public long getSalary() {
        return salary;
    }

    public long getCityId() {
        return cityId;
    }

    public String getDeptTitle() {
        return deptTitle;
    }
}
