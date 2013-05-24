/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import org.junit.Ignore;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

@Ignore("not a JUnit test")
public final class TestUtil {

    static final private SerializationService serializationService = new SerializationServiceImpl(1);

    public static Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    public static Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            impl = ((HazelcastInstanceProxy) hz).original;
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl != null ? impl.node : null;
    }

    public static class ValueType implements Serializable {
        String typeName;

        public ValueType(String typeName) {
            this.typeName = typeName;
        }

        public ValueType() {
        }

        public String getTypeName() {
            return typeName;
        }
    }

    public static class Value implements Serializable {
        String name;
        ValueType type;
        int index;

        public Value(String name, ValueType type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        public Value(String name, int index) {
            this.name = name;
            this.index = index;
        }

        public Value(String name) {
            this(name, null, 0);
        }

        public String getName() {
            return name;
        }

        public ValueType getType() {
            return type;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(final int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Value value = (Value) o;

            if (index != value.index) return false;
            if (name != null ? !name.equals(value.name) : value.name != null) return false;
            if (type != null ? !type.equals(value.type) : value.type != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + index;
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("Value");
            sb.append("{name=").append(name);
            sb.append(", index=").append(index);
            sb.append(", type=").append(type);
            sb.append('}');
            return sb.toString();
        }
    }


    public static enum State {
        STATE1, STATE2
    }

    public static class Employee implements Serializable {
        long id;
        String name;
        String city;
        int age;
        boolean active;
        double salary;
        Timestamp date;
        Date createDate;
        java.sql.Date sqlDate;
        State state;
        BigDecimal bigDecimal = new BigDecimal("1.23E3");

        public Employee(long id, String name, int age, boolean live, double salary, State state) {
            this.state = state;
        }

        public Employee(long id, String name, int age, boolean live, double salary) {
            this(id, name, null, age, live, salary);
        }

        public Employee(String name, int age, boolean live, double salary) {
            this(-1, name, age, live, salary);
        }

        public Employee(long id, String name, String city, int age, boolean live, double salary) {
            this.id = id;
            this.name = name;
            this.city = city;
            this.age = age;
            this.active = live;
            this.salary = salary;
            this.createDate = new Date();
            this.date = new Timestamp(createDate.getTime());
            this.sqlDate = new java.sql.Date(createDate.getTime());
        }

        public Employee() {
        }

        public BigDecimal getBigDecimal() {
            return bigDecimal;
        }

        public Timestamp getDate() {
            return date;
        }

        public String getName() {
            return name;
        }

        public String getCity() {
            return city;
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

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Employee employee = (Employee) o;
            if (active != employee.active) return false;
            if (age != employee.age) return false;
            if (Double.compare(employee.salary, salary) != 0) return false;
            if (name != null ? !name.equals(employee.name) : employee.name != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            result = 31 * result + (active ? 1 : 0);
            temp = salary != +0.0d ? Double.doubleToLongBits(salary) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("Employee");
            sb.append("{name='").append(name).append('\'');
            sb.append(", city=").append(city);
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }
    }
}

