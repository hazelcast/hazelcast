package com.hazelcast.map.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.mockito.Mockito;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueryIndexingTest extends HazelcastTestSupport {

    private int count = 5000;
    private Map<Integer, Employee> employees = newEmployees(count);
    private Config conf = newConfig(employees);

    private TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
    private HazelcastInstance h1 = nodeFactory.newHazelcastInstance(conf);
    private HazelcastInstance h2 = nodeFactory.newHazelcastInstance(conf);

    private EntryObject e = new PredicateBuilder().getEntryObject();
    private Predicate predicate = e.get("name").equal(null).and(e.get("city").isNull());

    @Before
    public void waitForCluster() {
        assertClusterSizeEventually(2, h1);
        waitClusterForSafeState(h1);
    }

    @Test
    public void testResultsHaveNullFields_whenPredicateTestsForNull() throws Exception {

        IMap<Integer, Employee> imap1 = h1.getMap("employees");
        imap1.putAll(employees);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.<String, Employee>getMap("employees"));

        assertEquals(count/2, matchingEntries.size());
        // N queries result in getters called N times
        assertGettersCalledNTimes(matchingEntries, 3);
        assertFieldsAreNull(matchingEntries);
    }

    @Test
    public void testResultsHaveNullFields_whenUsingIndexes()  throws Exception  {

        IMap<Integer, Employee> imap1 = h1.getMap("employees");

        imap1.addIndex("name", false);
        imap1.addIndex("city", true);

        imap1.putAll(employees);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.<String, Employee>getMap("employees"));
        assertEquals(count/2, matchingEntries.size());

        // N queries result in getters called only 1 time, when indexing
        assertGettersCalledNTimes(matchingEntries, 1);
        assertFieldsAreNull(matchingEntries);
    }

    private Collection<Employee> runQueryNTimes(int nTimes, IMap<String, Employee> imap2) {
        Collection<Employee> result = emptyList();
        for(int i = 0; i < nTimes; i++) {
            result = imap2.values(predicate);
        }
        return result;
    }

    private static void assertGettersCalledNTimes(Collection<Employee> matchingEmployees, int n) {
        for(Employee employee : matchingEmployees) {
            verify(employee, times(n)).getCity();
            verify(employee, times(n)).getName();
        }
    }

    private static void assertFieldsAreNull(Collection<Employee> matchingEmployees) {
        for(Employee employee : matchingEmployees) {
            assertNull("city", employee.getCity());
            assertNull("name", employee.getName());
        }
    }

    private static Config newConfig(final Map<Integer, Employee> employees) {
        Config conf = new Config();
        conf.getMapConfig("employees").setBackupCount(0);

        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setTypeClass(Employee.class);
        // serialize list index because employees are wrapped in Mockito.spy
        serializerConfig.setImplementation(new EmployeeIdSerializer(employees));

        conf.getSerializationConfig().addSerializerConfig(serializerConfig);
        return conf;
    }

    private static Map<Integer, Employee> newEmployees(int employeeCount) {
        Map<Integer, Employee> employees = new HashMap<Integer, Employee>();
        for (int i = 0; i < employeeCount; i++) {
            boolean hasNull = i % 2 == 0;
            Employee val = new Employee(i, hasNull ? null : "name" + i, hasNull ? null : "city" + i, i % 60, true, i);
            employees.put(i, Mockito.spy(val));
        }
        return employees;
    }

    /**
     * Serializes only employee id and deserializes to existing instance.
     * For testing purposes does not serialize actual objects.
     */
    static class EmployeeIdSerializer implements StreamSerializer<Employee> {

        private Map<Integer, Employee> employees;

        public EmployeeIdSerializer(Map<Integer, Employee> employees) {
            this.employees = employees;
        }

        @Override
        public void write(ObjectDataOutput out, Employee employee) throws IOException {
            out.writeInt((int)employee.getId());
        }

        @Override
        public Employee read(ObjectDataInput in) throws IOException {
            return employees.get(in.readInt());
        }

        @Override
        public int getTypeId() {
            return 123;
        }

        @Override
        public void destroy() {
        }
    }

}
