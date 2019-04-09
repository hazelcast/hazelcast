/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.Extractable;

import java.io.Serializable;

public class Employee implements Serializable, Extractable {
    public int age;
    public long iq;
    public int height;
    public int salary = 100;

    public Employee() {
    }

    public Employee(int age, int iq, int height) {
        this.age = age;
        this.iq = iq;
        this.height = height;
    }

    @Override
    public String toString() {
        return "Employee{"
                + "age=" + age
                + ", iq=" + iq
                + ", height=" + height
                + ", money=" + salary
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

        Employee employee = (Employee) o;

        if (age != employee.age) {
            return false;
        }
        if (iq != employee.iq) {
            return false;
        }
        if (height != employee.height) {
            return false;
        }
        return salary == employee.salary;
    }

    @Override
    public int hashCode() {
        int result = age;
        result = 31 * result + (int) (iq ^ (iq >>> 32));
        result = 31 * result + height;
        result = 31 * result + salary;
        return result;
    }

    @Override
    public Object getAttributeValue(String attributeName) throws QueryException {
        if (attributeName.equals("age")) {
            return age;
        } else if (attributeName.equals("iq")) {
            return iq;
        } else if (attributeName.equals("height")) {
            return height;
        } else if (attributeName.equals("salary")) {
            return salary;
        }
        return null;
    }

//    @Override
//    public AttributeType getAttributeType(String attributeName) throws QueryException {
//        if (attributeName.equals("age")) {
//            return AttributeType.INTEGER;
//        } else if (attributeName.equals("iq")) {
//            return AttributeType.LONG;
//        } else if (attributeName.equals("height")) {
//            return AttributeType.INTEGER;
//        } else if (attributeName.equals("salary")) {
//            return AttributeType.INTEGER;
//        }
//        return null;
//    }
}
