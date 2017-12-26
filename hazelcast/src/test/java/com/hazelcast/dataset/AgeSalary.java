package com.hazelcast.dataset;

public class AgeSalary {
    public int age;
    public int salary;

    @Override
    public String toString() {
        return "AgeSalary{" +
                "age=" + age +
                ", salary=" + salary +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgeSalary ageSalary = (AgeSalary) o;

        if (age != ageSalary.age) return false;
        return salary == ageSalary.salary;
    }

    @Override
    public int hashCode() {
        int result = age;
        result = 31 * result + salary;
        return result;
    }
}
