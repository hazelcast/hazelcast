package com.hazelcast.dataseries;

import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
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
        return "Employee{" +
                "age=" + age +
                ", iq=" + iq +
                ", height=" + height +
                ", money=" + salary +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Employee employee = (Employee) o;

        if (age != employee.age) return false;
        if (iq != employee.iq) return false;
        if (height != employee.height) return false;
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

    @Override
    public AttributeType getAttributeType(String attributeName) throws QueryException {
        if (attributeName.equals("age")) {
            return AttributeType.INTEGER;
        } else if (attributeName.equals("iq")) {
            return AttributeType.LONG;
        } else if (attributeName.equals("height")) {
            return AttributeType.INTEGER;
        } else if (attributeName.equals("salary")) {
            return AttributeType.INTEGER;
        }
        return null;
    }
}
