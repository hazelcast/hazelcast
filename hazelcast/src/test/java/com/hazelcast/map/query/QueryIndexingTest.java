package com.hazelcast.map.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

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

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueryIndexingTest extends HazelcastTestSupport {

    private int count = 2000;
    private Map<Integer, Employee> employees;
    private Config conf;

    private TestHazelcastInstanceFactory nodeFactory;
    private HazelcastInstance h1;
    private HazelcastInstance h2;

    private EntryObject e;
    private Predicate predicate;

    @Before
    public void waitForCluster() {
        employees = newEmployees(count);
        conf = newConfig(employees);

        nodeFactory = createHazelcastInstanceFactory(2);
        h1 = nodeFactory.newHazelcastInstance(conf);
        h2 = nodeFactory.newHazelcastInstance(conf);

        e = new PredicateBuilder().getEntryObject();
        predicate = e.get("name").equal(null).and(e.get("city").isNull());

        assertClusterSizeEventually(2, h1);
    }

    @Test
    public void testResultsHaveNullFields_whenPredicateTestsForNull() throws Exception {

        IMap<Integer, Employee> imap1 = h1.getMap("employees");
        imap1.putAll(employees);
        waitAllForSafeState();

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
        waitAllForSafeState();

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
        conf.getMapConfig("employees").setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0);
        return conf;
    }

    private static Map<Integer, Employee> newEmployees(int employeeCount) {
        Map<Integer, Employee> employees = new HashMap<Integer, Employee>();
        for (int i = 0; i < employeeCount; i++) {
            Employee val;
            if(i % 2 == 0) {
                val = new Employee(i, null, null, 0, true, i);
            } else {
                val = new Employee(i, "name" + i, "city" + i, 0, true, i);
            }
            Employee spy = MockUtil.serializableSpy(Employee.class, val);
            employees.put(i, spy);
        }
        return employees;
    }

}
