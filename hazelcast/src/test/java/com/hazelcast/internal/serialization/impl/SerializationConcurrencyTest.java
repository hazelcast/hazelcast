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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SerializationConcurrencyTest {

    public static final short FACTORY_ID = 1;

    @Test
    public void test() throws IOException, InterruptedException {
        PortableFactory portableFactory = new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case 1:
                        return new PortablePerson();
                    case 2:
                        return new PortableAddress();
                    default:
                        throw new IllegalArgumentException();
                }
            }
        };
        final SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, portableFactory)
                .build();

        final int k = 10;
        final AtomicBoolean error = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(k);
        ExecutorService ex = Executors.newCachedThreadPool();
        for (int i = 0; i < k; i++) {
            ex.execute(new Runnable() {
                final Random rand = new Random();

                public void run() {
                    try {
                        for (int j = 0; j < 10000; j++) {
                            String key = "key" + rnd();
                            Data dataKey = ss.toData(key);
                            Assert.assertEquals(key, ss.toObject(dataKey));

                            Long value = 123L + rnd();
                            Data dataValue = ss.toData(value);
                            Assert.assertEquals(value, ss.toObject(dataValue));

                            Address address = new Address("here here" + rnd(), 13131 + rnd());
                            Data dataAddress = ss.toData(address);
                            Assert.assertEquals(address, ss.toObject(dataAddress));

                            Person person = new Person(13 + rnd(), 199L + rnd(), 56.89d, "mehmet", address);
                            Data dataPerson = ss.toData(person);
                            Assert.assertEquals(person, ss.toObject(dataPerson));

                            PortableAddress portableAddress = new PortableAddress("there there " + rnd(), 90909 + rnd());
                            Data dataPortableAddress = ss.toData(portableAddress);
                            Assert.assertEquals(portableAddress, ss.toObject(dataPortableAddress));

                            PortablePerson portablePerson = new PortablePerson(
                                    63 + rnd(), 167L + rnd(), "ahmet", portableAddress);
                            Data dataPortablePerson = ss.toData(portablePerson);
                            Assert.assertEquals(portablePerson, ss.toObject(dataPortablePerson));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        error.set(true);
                    } finally {
                        latch.countDown();
                    }
                }

                int rnd() {
                    return rand.nextInt();
                }
            });
        }
        latch.await();
        ex.shutdown();

        if (error.get()) {
            throw new AssertionError();
        }
    }

    public static class Address implements DataSerializable {

        private String street;

        private int no;

        public Address() {
        }

        public Address(String street, int no) {
            this.street = street;
            this.no = no;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(street);
            out.writeInt(no);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            street = in.readString();
            no = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Address address = (Address) o;

            if (no != address.no) {
                return false;
            }
            if (street != null ? !street.equals(address.street) : address.street != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = street != null ? street.hashCode() : 0;
            result = 31 * result + no;
            return result;
        }
    }

    public static class Person implements DataSerializable, Delayed {

        private int age;

        private long height;

        private double weight;

        private String name;

        private Address address;

        public Person() {
        }

        public Person(int age, long height, double weight, String name, Address address) {
            this.age = age;
            this.height = height;
            this.weight = weight;
            this.name = name;
            this.address = address;
        }

        public int getAge() {
            return age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeObject(address);
            out.writeInt(age);
            out.writeLong(height);
            out.writeDouble(weight);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readString();
            address = in.readObject();
            age = in.readInt();
            height = in.readLong();
            weight = in.readDouble();
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

            if (age != person.age) {
                return false;
            }
            if (height != person.height) {
                return false;
            }
            if (Double.compare(person.weight, weight) != 0) {
                return false;
            }
            if (address != null ? !address.equals(person.address) : person.address != null) {
                return false;
            }
            if (name != null ? !name.equals(person.name) : person.name != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = age;
            result = 31 * result + (int) (height ^ (height >>> 32));
            temp = weight != +0.0d ? Double.doubleToLongBits(weight) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }

        @Override
        public int compareTo(Delayed o) {
            Person other = (Person) o;

            return Integer.compare(age, other.age);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public String toString() {
            return "Person{" + "age=" + age + ", height=" + height + ", weight=" + weight + ", name='" + name + '\''
                    + ", address=" + address + '}';
        }
    }

    public static class PortableAddress implements Portable {

        private String street;

        private int no;

        public PortableAddress() {
        }

        public PortableAddress(String street, int no) {
            this.street = street;
            this.no = no;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("no", no);
            writer.writeString("street", street);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            street = reader.readString("street");
            no = reader.readInt("no");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PortableAddress that = (PortableAddress) o;
            if (no != that.no) {
                return false;
            }
            if (street != null ? !street.equals(that.street) : that.street != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = street != null ? street.hashCode() : 0;
            result = 31 * result + no;
            return result;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    public static class PortablePersonComparator implements Comparator<PortablePerson>, Portable {
        public static final int CLASS_ID = 4;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer)
                throws IOException {
        }

        @Override
        public void readPortable(PortableReader reader)
                throws IOException {
        }

        @Override
        public int compare(PortablePerson o1, PortablePerson o2) {
            return Integer.compare(o1.age, o2.age);
        }
    }

    public static class PortableIntegerComparator implements Comparator<Integer>, Portable {
        public static final int CLASS_ID = 3;

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer)
                throws IOException {
        }

        @Override
        public void readPortable(PortableReader reader)
                throws IOException {
        }
    }

    public static class PortablePerson implements Portable, Delayed {

        private int age;

        private long height;

        private String name;

        private PortableAddress address;

        public PortablePerson() {
        }

        public PortablePerson(int age, long height, String name, PortableAddress address) {
            this.age = age;
            this.height = height;
            this.name = name;
            this.address = address;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("height", height);
            writer.writeInt("age", age);
            writer.writeString("name", name);
            writer.writePortable("address", address);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            address = reader.readPortable("address");
            height = reader.readLong("height");
            age = reader.readInt("age");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PortablePerson that = (PortablePerson) o;

            if (age != that.age) {
                return false;
            }
            if (height != that.height) {
                return false;
            }
            if (address != null ? !address.equals(that.address) : that.address != null) {
                return false;
            }
            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = age;
            result = 31 * result + (int) (height ^ (height >>> 32));
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            PortablePerson other = (PortablePerson) o;

            return Integer.compare(age, other.age);
        }
    }
}
