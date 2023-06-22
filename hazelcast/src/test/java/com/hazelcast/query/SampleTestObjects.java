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

package com.hazelcast.query;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class SampleTestObjects {

    public static class PortableEmployee implements Portable {

        private int age;
        private String name;

        public PortableEmployee() {
        }

        public PortableEmployee(int age, String name) {
            this.age = age;
            this.name = name;
        }

        @Override
        public int getFactoryId() {
            return 666;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("n", name);
            writer.writeInt("a", age);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("n");
            age = reader.readInt("a");
        }

        @Override
        public String toString() {
            return "PortableEmployee{"
                    + "age=" + age
                    + ", name='" + name + '\''
                    + '}';
        }
    }

    public static class ValueType implements Serializable, Comparable<ValueType> {

        String typeName;

        public ValueType(String typeName) {
            this.typeName = typeName;
        }

        public ValueType() {
        }

        public String getTypeName() {
            return typeName;
        }

        @Override
        public int compareTo(ValueType o) {
            if (o == null) {
                return 1;
            }
            if (typeName == null) {
                if (o.typeName == null) {
                    return 0;
                } else {
                    return -1;
                }
            }
            if (o.typeName == null) {
                return 1;
            }
            return typeName.compareTo(o.typeName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ValueType valueType = (ValueType) o;

            return !(typeName != null ? !typeName.equals(valueType.typeName) : valueType.typeName != null);
        }

        @Override
        public int hashCode() {
            return typeName != null ? typeName.hashCode() : 0;
        }
    }

    public static class Value implements Serializable {

        String name;
        ValueType type;
        State state;
        int index;

        public Value(String name, ValueType type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        public Value(State state, ValueType type, int index) {
            this.state = state;
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

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Value value = (Value) o;

            if (index != value.index) {
                return false;
            }
            if (name != null ? !name.equals(value.name) : value.name != null) {
                return false;
            }
            if (type != null ? !type.equals(value.type) : value.type != null) {
                return false;
            }

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
            return "Value{"
                    + "name=" + name
                    + ", index=" + index
                    + ", type=" + type
                    + '}';
        }
    }

    public enum State {
        STATE1,
        STATE2
    }

    @SuppressWarnings("unused")
    public static class Employee implements Serializable {

        long id;
        String name;
        String city;
        int age;
        boolean active;
        double salary;
        Timestamp date;
        Date createDate;
        State state;

        public Employee(long id, String name, int age, boolean live, double salary, State state) {
            this(id, name, age, live, salary);
            this.state = state;
        }

        public Employee(long id, String name, int age, boolean live, double salary) {
            this(id, name, null, age, live, salary);
        }

        public Employee(String name, int age, boolean live, double salary) {
            this(-1, name, age, live, salary);
        }

        public Employee(String name, String city, int age, boolean live, double salary) {
            this(-1, name, city, age, live, salary);
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
        }

        public Employee() {
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public Date getCreateDate() {
            return createDate;
        }

        public void setCreateDate(Date createDate) {
            this.createDate = createDate;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public void setSalary(double salary) {
            this.salary = salary;
        }

        public void setDate(Timestamp date) {
            this.date = date;
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Employee employee = (Employee) o;
            if (active != employee.active) {
                return false;
            }
            if (age != employee.age) {
                return false;
            }
            if (Double.compare(employee.salary, salary) != 0) {
                return false;
            }
            if (name != null ? !name.equals(employee.name) : employee.name != null) {
                return false;
            }
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
            return "Employee{"
                    + "name='" + name + '\''
                    + ", city=" + city
                    + ", age=" + age
                    + ", active=" + active
                    + ", salary=" + salary
                    + '}';
        }
    }

    // Mockito encounters non-deterministic failures in some tests when mocking
    // the Employee object - to avoid further time spent investigating that issue,
    // we simply provide our own basic spied invocation counter instead
    public static class SpiedEmployee extends Employee {
        private final Map<String, AtomicInteger> invocationCounters = new ConcurrentHashMap<>();

        public SpiedEmployee(long id, String name, int age, boolean live, double salary, State state) {
            super(id, name, age, live, salary, state);
        }

        public SpiedEmployee(long id, String name, int age, boolean live, double salary) {
            super(id, name, age, live, salary);
        }

        public SpiedEmployee(String name, int age, boolean live, double salary) {
            super(name, age, live, salary);
        }

        public SpiedEmployee(String name, String city, int age, boolean live, double salary) {
            super(name, city, age, live, salary);
        }

        public SpiedEmployee(long id, String name, String city, int age, boolean live, double salary) {
            super(id, name, city, age, live, salary);
        }

        public SpiedEmployee() {
        }

        public long getId() {
            invocationCounters.computeIfAbsent("getId", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getId();
        }

        public Date getCreateDate() {
            invocationCounters.computeIfAbsent("getCreateDate", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getCreateDate();
        }

        public Timestamp getDate() {
            invocationCounters.computeIfAbsent("getDate", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getDate();
        }

        public String getName() {
            invocationCounters.computeIfAbsent("getName", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getName();
        }

        public String getCity() {
            invocationCounters.computeIfAbsent("getCity", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getCity();
        }

        public int getAge() {
            invocationCounters.computeIfAbsent("getAge", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getAge();
        }

        public double getSalary() {
            invocationCounters.computeIfAbsent("getSalary", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getSalary();
        }

        public boolean isActive() {
            invocationCounters.computeIfAbsent("isActive", i -> new AtomicInteger(0)).getAndIncrement();
            return super.isActive();
        }

        public State getState() {
            invocationCounters.computeIfAbsent("getState", i -> new AtomicInteger(0)).getAndIncrement();
            return super.getState();
        }

        public int getInvocationCount(String methodName) {
            AtomicInteger counter = invocationCounters.get(methodName);
            return counter == null ? 0 : counter.get();
        }
    }

    public static class ObjectWithInteger implements Serializable {
        private int attribute;

        public ObjectWithInteger(int attribute) {
            this.attribute = attribute;
        }

        public int getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithLong implements Serializable {
        private long attribute;

        public ObjectWithLong(long attribute) {
            this.attribute = attribute;
        }

        public long getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithShort implements Serializable {
        private short attribute;

        public ObjectWithShort(short attribute) {
            this.attribute = attribute;
        }

        public short getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithByte implements Serializable {
        private byte attribute;

        public ObjectWithByte(byte attribute) {
            this.attribute = attribute;
        }

        public byte getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithFloat implements Serializable {
        private float attribute;

        public ObjectWithFloat(float attribute) {
            this.attribute = attribute;
        }

        public float getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithDouble implements Serializable {
        private double attribute;

        public ObjectWithDouble(double attribute) {
            this.attribute = attribute;
        }

        public double getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithChar implements Serializable {
        private char attribute;

        public ObjectWithChar(char attribute) {
            this.attribute = attribute;
        }

        public char getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithBoolean implements Serializable {
        private boolean attribute;

        public ObjectWithBoolean(boolean attribute) {
            this.attribute = attribute;
        }

        public boolean getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithBigDecimal implements Serializable {
        private BigDecimal attribute;

        public ObjectWithBigDecimal(BigDecimal attribute) {
            this.attribute = attribute;
        }

        public BigDecimal getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithBigInteger implements Serializable {
        private BigInteger attribute;

        public ObjectWithBigInteger(BigInteger attribute) {
            this.attribute = attribute;
        }

        public BigInteger getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithSqlTimestamp implements Serializable {
        private Timestamp attribute;

        public ObjectWithSqlTimestamp(Timestamp attribute) {
            this.attribute = attribute;
        }

        public Timestamp getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithSqlDate implements Serializable {
        private java.sql.Date attribute;

        public ObjectWithSqlDate(java.sql.Date attribute) {
            this.attribute = attribute;
        }

        public java.sql.Date getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithDate implements Serializable {
        private Date attribute;

        public ObjectWithDate(Date attribute) {
            this.attribute = attribute;
        }

        public Date getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithUUID implements Serializable {
        private UUID attribute;

        public ObjectWithUUID(UUID attribute) {
            this.attribute = attribute;
        }

        public UUID getAttribute() {
            return attribute;
        }
    }

    public static class ObjectWithOptional<T> implements Serializable {
        private T attribute;

        public ObjectWithOptional(T attribute) {
            this.attribute = attribute;
        }

        public Optional<T> getAttribute() {
            return Optional.ofNullable(attribute);
        }
    }

}
