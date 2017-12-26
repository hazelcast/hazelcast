package com.hazelcast.dataseries;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class IncreaseAgeSalary implements RecordMutator<Employee>{

    @Override
    public boolean mutate(Employee object) {
        object.age++;
        object.salary++;
        return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
