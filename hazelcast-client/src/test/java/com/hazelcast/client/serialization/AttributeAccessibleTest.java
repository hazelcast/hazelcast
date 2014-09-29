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

package com.hazelcast.client.serialization;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.AttributeAccessible;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * This test creates a server that has a generic serializer and a client that has a specific serializer and checks
 * if the server can index and query the data the client sends.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AttributeAccessibleTest {

    private static HazelcastInstance client;
    private static PropertySerializer serializer;

    private static final int CUSTOMER_TYPE_ID = 10000;

    @BeforeClass
    public static void init() {
        Config serverConf = new Config();
        // The server gets the unspecific serializer
        serializer = new PropertySerializer(CUSTOMER_TYPE_ID);
        serverConf.getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setTypeClass(PropertyAttributeAccessible.class)
                        .setImplementation(serializer));
        Hazelcast.newHazelcastInstance(serverConf);

        ClientConfig clientConf = new ClientConfig();
        // The client gets the specific serializer
        clientConf.getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setTypeClass(TestCustomer.class)
                        .setImplementation(new TestCustomerSerializer()));
        client = HazelcastClient.newHazelcastClient(clientConf);
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void indexingTest() {
        serializer.resetDeserialized();
        IMap<String, TestCustomer> customers = client.getMap("customers");
        customers.addIndex("address.street", false);
        // Put in two test customers (After putting the index)
        final TestCustomer customer1 = addTestCustomersAndReturnFirst(customers);

        // Now the deserialize method has been called (during indexing)
        checkDeserializationAccessPattern(1);

        serializer.resetDeserialized();
        checkCustomerSearchOk(customers, customer1);
        // During the query nothing was deserialized since we only queried the field that is indexed
        assertThat(serializer.getDeserialized().size(), is(0));

        customers.destroy();
    }

    @Test
    public void queryingTest() {
        serializer.resetDeserialized();
        IMap<String, TestCustomer> customers = client.getMap("customers");
        // Put in two test customers (without putting the index first)
        final TestCustomer customer1 = addTestCustomersAndReturnFirst(customers);

        // There is no indexing now so the deserialization is not called during insertion
        assertThat(serializer.getDeserialized().size(), is(0));
        checkCustomerSearchOk(customers, customer1);

        // But deserialization is called when querying
        checkDeserializationAccessPattern(2);

        customers.destroy();
    }

    /**
     * Add two test customers and return first one.
     *
     * @param customers List of customers to add to
     * @return First created customer
     */
    private TestCustomer addTestCustomersAndReturnFirst(IMap<String, TestCustomer> customers) {
        final TestCustomer customer1 = new TestCustomer(new TestAddress("City1", "Street1"), "Name1");
        customers.put(customer1.getName(), customer1);
        final TestCustomer customer2 = new TestCustomer(new TestAddress("City2", "Street2"), "Name2");
        customers.put(customer2.getName(), customer2);
        return customer1;
    }

    /**
     * Check that customer1 is found using search and that search doesn't find invalid customer
     *
     * @param customers Customer map to search
     * @param customer1 Customer to find
     */
    private void checkCustomerSearchOk(IMap<String, TestCustomer> customers, TestCustomer customer1) {
        // Check that the returned data actually only contains the one customer we are looking for
        EntryObject e = new PredicateBuilder().getEntryObject();
        final PredicateBuilder predicate = e.get("address.street").equal(customer1.getAddress().getStreet());
        final Collection<TestCustomer> customers1 = customers.values(predicate);
        assertThat(customers1.size(), is(1));
        assertThat(customers1.iterator().next(), is(customer1));

        // Check that if we are looking for a customer that we didn't put in we get an empty result
        EntryObject eEmpty = new PredicateBuilder().getEntryObject();
        final PredicateBuilder predicateEmpty = eEmpty.get("address.street").equal("StreetNonExistent");
        final Collection<TestCustomer> customersEmpty = customers.values(predicateEmpty);
        assertThat(customersEmpty.size(), is(0));
    }

    /**
     * Check that the deserializer has been accessed the correct number of times.
     *
     * @param factor Factor for access numbers. While there is only one access per object in indexing there are multiple
     *               accesses in querying if there is no index.
     */
    private void checkDeserializationAccessPattern(int factor) {
        final List<PropertyAttributeAccessible> deserialized = serializer.getDeserialized();
        assertThat(deserialized.size(), is(2 * factor));
        final Map<String, Integer> accessPatternExpected = new HashMap<String, Integer>();
        // The access pattern is the same for both customers. The street attribute was accessed once (for indexing)
        accessPatternExpected.put("Name1:address.street", factor);
        accessPatternExpected.put("Name2:address.street", factor);
        assertThat(serializer.getAccessMap(), is(accessPatternExpected));
    }

    /**
     * This is the generic property serializer
     */
    private static class PropertySerializer implements ByteArraySerializer<Object> {
        private int typeId;
        /**
         * Objects that have been deserialized
         */
        private final List<PropertyAttributeAccessible> deserialized = new Vector<PropertyAttributeAccessible>();
        /**
         * Number of times that each property of the deserialized objects has been accessed
         */
        private final Map<String, Integer> accessMap = new ConcurrentHashMap<String, Integer>();

        public PropertySerializer(int typeId) {
            this.typeId = typeId;
        }

        @Override
        public byte[] write(Object object) throws IOException {
            throw new UnsupportedOperationException("This test serializer can only deserialize");
        }

        @Override
        public Object read(byte[] buffer) throws IOException {
            final Properties properties = new Properties();
            properties.load(new ByteArrayInputStream(buffer));
            final PropertyAttributeAccessible deserializedEnt = new PropertyAttributeAccessible(properties, this);
            deserialized.add(deserializedEnt);
            return deserializedEnt;
        }

        @Override
        public int getTypeId() {
            return typeId;
        }

        @Override
        public void destroy() {
        }

        /**
         * Count access to getValue on objects that has been deserialized by this serializer
         */
        public void countAccess(String name, String attribute) {
            String key = name + ":" + attribute;
            // Log access
            synchronized (accessMap) {
                if (!accessMap.containsKey(key)) {
                    accessMap.put(key, 0);
                }
                accessMap.put(key, accessMap.get(key) + 1);
            }
        }

        /**
         * @return List of objects that have been deserialized using this serializer
         */
        public List<PropertyAttributeAccessible> getDeserialized() {
            return deserialized;
        }

        /**
         * @return Map of attributes of deserialized objects and how often they were accessed. Key is [name of object]:[attribute name]
         */
        public Map<String, Integer> getAccessMap() {
            return accessMap;
        }

        /**
         * Reset list of deserialized objects and access list
         */
        public void resetDeserialized() {
            deserialized.clear();
            accessMap.clear();
        }
    }

    /**
     * This is an example implementation of a custom AttributeAccessible type (for java properties files)
     */
    private static class PropertyAttributeAccessible implements AttributeAccessible {
        private final Properties baseProps;
        private PropertySerializer propertySerializer;

        private PropertyAttributeAccessible(Properties baseProps, PropertySerializer propertySerializer) {
            this.baseProps = baseProps;
            this.propertySerializer = propertySerializer;
        }

        @Override
        public Object getValue(String attributeName) {
            // Log access
            propertySerializer.countAccess(baseProps.getProperty("name"), attributeName);
            // And return
            return baseProps.getProperty(attributeName);
        }

        @Override
        public Class<?> getType(String attributeName) {
            // Properties are always of type String
            return String.class;
        }
    }

    /**
     * This is the specific class that only the client knows about
     */
    private static class TestCustomer {
        private TestAddress address;
        private String name;

        private TestCustomer(TestAddress address, String name) {
            this.address = address;
            this.name = name;
        }

        public TestAddress getAddress() {
            return address;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestCustomer that = (TestCustomer) o;

            return address.equals(that.address) && name.equals(that.name);

        }

        @Override
        public int hashCode() {
            int result = address.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }
    }

    /**
     * Inner content of TestCustomer
     */
    private static class TestAddress {
        private String city;
        private String street;

        private TestAddress(String city, String street) {
            this.city = city;
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public String getStreet() {
            return street;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestAddress that = (TestAddress) o;

            return city.equals(that.city) && street.equals(that.street);

        }

        @Override
        public int hashCode() {
            int result = city.hashCode();
            result = 31 * result + street.hashCode();
            return result;
        }
    }

    /**
     * Tis is the specific version of the serializer that knows about the Customer class
     */
    private static class TestCustomerSerializer implements ByteArraySerializer<TestCustomer> {
        @Override
        public byte[] write(TestCustomer object) throws IOException {
            Properties properties = new Properties();
            properties.setProperty("name", object.getName());
            properties.setProperty("address.city", object.getAddress().getCity());
            properties.setProperty("address.street", object.getAddress().getStreet());

            ByteArrayOutputStream stream = new ByteArrayOutputStream(1000);
            properties.store(stream, null);
            stream.close();
            return stream.toByteArray();
        }

        @Override
        public TestCustomer read(byte[] buffer) throws IOException {
            final Properties properties = new Properties();
            properties.load(new ByteArrayInputStream(buffer));

            TestAddress addr = new TestAddress(properties.getProperty("address.city"), properties.getProperty("address.street"));
            return new TestCustomer(addr, properties.getProperty("name"));
        }

        @Override
        public int getTypeId() {
            return CUSTOMER_TYPE_ID;
        }

        @Override
        public void destroy() {
        }
    }
}