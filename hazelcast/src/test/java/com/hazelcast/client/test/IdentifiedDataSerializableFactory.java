/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */

package com.hazelcast.client.test;

import com.hazelcast.client.test.ringbuffer.filter.StartsWithStringFilter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class IdentifiedDataSerializableFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 666;

    class SampleFailingTask implements Callable, IdentifiedDataSerializable {

        SampleFailingTask() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return 1;
        }

        public String call() throws Exception {
            throw new IllegalStateException();
        }

        public void writeData(ObjectDataOutput out) throws IOException {
        }

        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    class SampleRunnableTask implements Portable, Runnable {

        private String name;

        SampleRunnableTask() {
        }

        public void run() {
            System.out.println("Running " + name);
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return 1;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("n", name);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("n");
        }
    }

    class SampleCallableTask implements IdentifiedDataSerializable, Callable {

        private String param;

        SampleCallableTask() {
        }

        public Object call() throws Exception {
            return param + ":result";
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return 2;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(param);
        }

        public void readData(ObjectDataInput in) throws IOException {
            param = in.readString();
        }
    }

    class KeyMultiplier implements IdentifiedDataSerializable, EntryProcessor<Integer, Employee, Integer> {
        private int multiplier;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 3;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeInt(multiplier);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            multiplier = in.readInt();
        }

        @Override
        public Integer process(Map.Entry<Integer, Employee> entry) {
            if (null == entry.getValue()) {
                return -1;
            }
            return multiplier * entry.getKey();
        }

        @Override
        public EntryProcessor<Integer, Employee, Integer> getBackupProcessor() {
            return null;
        }
    }

    class WaitMultiplierProcessor
            implements IdentifiedDataSerializable, EntryProcessor<Integer, Employee, Integer> {
        private int waiTimeInMillis;
        private int multiplier;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 8;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeInt(waiTimeInMillis);
            out.writeInt(multiplier);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            waiTimeInMillis = in.readInt();
            multiplier = in.readInt();
        }

        @Override
        public Integer process(Map.Entry<Integer, Employee> entry) {
            try {
                Thread.sleep(waiTimeInMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (null == entry.getValue()) {
                return -1;
            }
            return multiplier * entry.getKey();
        }

        @Override
        public EntryProcessor<Integer, Employee, Integer> getBackupProcessor() {
            return null;
        }
    }

    class KeyMultiplierWithNullableResult extends KeyMultiplier {
        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 7;
        }

        @Override
        public Integer process(Map.Entry<Integer, Employee> entry) {
            if (null == entry.getValue()) {
                return null;
            }
            return super.process(entry);
        }
    }

    class PartitionAwareInt implements IdentifiedDataSerializable {
        private int value;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 9;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput)
                throws IOException {
            objectDataOutput.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput)
                throws IOException {
            value = objectDataInput.readInt();
        }
    }

    /**
     * Compares based on the employee age
     */
    class EmployeeEntryComparator implements IdentifiedDataSerializable, Comparator<Map.Entry<Integer, Employee>> {
        private int multiplier;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 4;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
        }

        @Override
        public int compare(Map.Entry<Integer, Employee> lhs, Map.Entry<Integer, Employee> rhs) {
            Employee lv;
            Employee rv;
            try {
                lv = lhs.getValue();
                rv = rhs.getValue();
            } catch (ClassCastException e) {
                return -1;
            }

            if (null == lv && null == rv) {
                // order by key
                int leftKey = lhs.getKey();
                int rightKey = rhs.getKey();

                if (leftKey == rightKey) {
                    return 0;
                }

                if (leftKey < rightKey) {
                    return -1;
                }

                return 1;
            }

            if (null == lv) {
                return -1;
            }

            if (null == rv) {
                return 1;
            }

            Integer la = lv.getAge();
            Integer ra = rv.getAge();

            return la.compareTo(ra);
        }
    }

    class EmployeeEntryKeyComparator extends EmployeeEntryComparator {
        @Override
        public int getClassId() {
            return 5;
        }

        @Override
        public int compare(Map.Entry<Integer, Employee> lhs, Map.Entry<Integer, Employee> rhs) {
            Integer key1 = lhs.getKey();
            Integer key2 = rhs.getKey();

            if (null == key1) {
                return -1;
            }

            if (null == key2) {
                return 1;
            }

            if (key1 == key2) {
                return 0;
            }

            if (key1 < key2) {
                return -1;
            }

            return 1;
        }
    }

    class UTFValueValidatorProcessor
            implements EntryProcessor<String, String, Boolean>, IdentifiedDataSerializable {
        @Override
        public Boolean process(Map.Entry<String, String> entry) {
            return entry.getKey().equals("myutfkey") && entry.getValue().equals("xyzä123 イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム");
        }

        @Override
        public EntryProcessor<String, String, Boolean> getBackupProcessor() {
            return null;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 9;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput)
                throws IOException {
        }

        @Override
        public void readData(ObjectDataInput objectDataInput)
                throws IOException {
        }
    }

    class MapGetInterceptor implements MapInterceptor, IdentifiedDataSerializable {
        private String prefix;

        @Override
        public Object interceptGet(Object value) {
            if (null == value) {
                return prefix;
            }

            String val = (String) value;
            return prefix + val;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 6;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeString(prefix);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            prefix = in.readString();
        }
    }

    class BaseDataSerializable implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 10;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput)
                throws IOException {
        }

        @Override
        public void readData(ObjectDataInput objectDataInput)
                throws IOException {
        }
    }

    class Derived1DataSerializable extends BaseDataSerializable {
        @Override
        public int getClassId() {
            return 11;
        }
    }

    class Derived2DataSerializable extends Derived1DataSerializable {
        @Override
        public int getClassId() {
            return 12;
        }
    }

    public static class CallableSignalsRunAndSleep implements Callable, IdentifiedDataSerializable, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;
        private String startSignalLatchName;

        public CallableSignalsRunAndSleep() {

        }

        public CallableSignalsRunAndSleep(String startSignalLatchName) {
            this.startSignalLatchName = startSignalLatchName;
        }

        @Override
        public Object call() {
            hazelcastInstance.getCPSubsystem().getCountDownLatch("callableStartedLatch").countDown();
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 13;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(startSignalLatchName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            startSignalLatchName = in.readString();
        }
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case 1:
                return new SampleFailingTask();
            case 2:
                return new SampleCallableTask();
            case 3:
                return new KeyMultiplier();
            case 4:
                return new EmployeeEntryComparator();
            case 5:
                return new EmployeeEntryKeyComparator();
            case 6:
                return new MapGetInterceptor();
            case 7:
                return new KeyMultiplierWithNullableResult();
            case 8:
                return new WaitMultiplierProcessor();
            case 9:
                return new UTFValueValidatorProcessor();
            case 10:
                return new BaseDataSerializable();
            case 11:
                return new Derived1DataSerializable();
            case 12:
                return new Derived2DataSerializable();
            case 13:
                return new CallableSignalsRunAndSleep();
            case StartsWithStringFilter.CLASS_ID:
                return new StartsWithStringFilter();
            default:
                return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }
}
