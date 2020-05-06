package com.hazelcast.sql.schema.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

@SuppressWarnings("unused")
public class Person implements Serializable {

    private String name;
    private BigInteger age;

    public Person() {
    }

    public Person(String name, BigInteger age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(BigInteger age) {
        this.age = age;
    }

    public BigInteger getAge() {
        return age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Person person = (Person) o;
        return Objects.equals(name, person.name) &&
                Objects.equals(age, person.age);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }
}