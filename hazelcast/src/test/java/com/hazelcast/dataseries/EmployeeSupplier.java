package com.hazelcast.dataseries;

import com.hazelcast.util.function.Supplier;

import java.io.Serializable;

class EmployeeSupplier implements Supplier<Employee>, Serializable {
    @Override
    public Employee get() {
        Employee employee = new Employee();
        long r = System.nanoTime();
        employee.age = (int)(r % 10000);
        employee.iq = r % 100000;
        return employee;
    }
}
