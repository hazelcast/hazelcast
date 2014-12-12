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
import java.util.UUID;

/**
 * @author mdogan 6/6/13
 */
public final class SampleObjects {

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
            writer.writeUTF("n", name);
            writer.writeInt("a", age);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readUTF("n");
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
            final StringBuilder sb = new StringBuilder();
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
}
