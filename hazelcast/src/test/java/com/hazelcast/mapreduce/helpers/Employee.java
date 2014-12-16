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