package com.hazelcast.query.impl;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.TestUtil.toData;
import static org.junit.Assert.assertEquals;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/14 10:39.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IndexServiceStoreTest {
    private void fillIndexes(IndexService service) {
        for (int i = 0; i < 1000; i++) {
            SampleObjects.Employee employee = new SampleObjects.Employee("activeEmployee" + i, i, true, Double.valueOf(i));
            service.saveEntryIndex(new QueryEntry(null, toData("activeEmployee" + i), "activeEmployee" + i, employee));
        }

        for (int i = 0; i < 1000; i++) {
            SampleObjects.Employee employee = new SampleObjects.Employee("passiveEmployee" + i, i, false, Double.valueOf(i));
            service.saveEntryIndex(new QueryEntry(null, toData("passiveEmployee" + i), "passiveEmployee" + i, employee));
        }
    }


    @Test(timeout = 60000)
    public void testStore() throws Exception {
        IndexService indexService = new IndexService();

        Index index = indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 50));

        fillIndexes(indexService);

        assertEquals(51, index.getRecordCount());
    }

    @Test(timeout = 60000)
    public void test_whenNotConditionalUnOrdered() throws Exception {

        IndexService indexService = new IndexService();
        Index salary = indexService.addOrGetIndex("salary", false);

        fillIndexes(indexService);

        assertEquals(1000, salary.getRecordCount());
    }

    @Test(timeout = 60000)
    public void test_whenNotConditionalOrdered() throws Exception {

        IndexService indexService = new IndexService();
        Index age = indexService.addOrGetIndex("age", true);

        fillIndexes(indexService);

        assertEquals(1000, age.getRecordCount());
    }

    @Test(timeout = 60000)
    public void test_whenOrdered() throws Exception {

        IndexService indexService = new IndexService();
        Index index = indexService.addOrGetIndex("age", true, Predicates.between("salary", 0, 100));

        fillIndexes(indexService);

        assertEquals(101, index.getRecordCount());
    }

    @Test(timeout = 60000)
    public void testQuery_whenComplexIndexCondition() throws Exception {

        IndexService indexService = new IndexService();
        Predicate predicate = Predicates.and(
                Predicates.between("age", 0, 100),
                Predicates.and(Predicates.between("salary", 0, 40),
                        Predicates.equal("active", true)));

        Index age = indexService.addOrGetIndex("age", false, predicate);

        fillIndexes(indexService);

        assertEquals(41, age.getRecordCount());
    }


    // todo: what should we do when index predicate can't be applied to the entries?
    @Test(timeout = 60000, expected = ClassCastException.class)
    public void testQuery_whenIllegalIndexCondition() throws Exception {

        IndexService indexService = new IndexService();
        Predicate predicate = Predicates.regex("active", ".*");

        indexService.addOrGetIndex("age", false, predicate);

        fillIndexes(indexService);
    }
}
