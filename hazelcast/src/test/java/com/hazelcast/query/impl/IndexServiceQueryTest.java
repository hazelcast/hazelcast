package com.hazelcast.query.impl;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.toData;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/14 10:47.
 */
public class IndexServiceQueryTest extends HazelcastTestSupport {


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
    public void testQueryAnd_whenSomeIndexesPredicate() throws Exception {
        IndexService indexService = new IndexService();

        Predicate salaryPredicate = Predicates.between("salary", 0, 100);

        indexService.addOrGetIndex("age", false, salaryPredicate);
        indexService.addOrGetIndex("age", false);

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                salaryPredicate,
                Predicates.greaterEqual("age", 50),
                Predicates.notEqual("name", "passiveEmployee50"));
        Set<QueryableEntry> query = indexService.query(and);

        assertNotNull(query);
        assertIterableSize(101, query);
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenIndexesNotMatch() throws Exception {
        IndexService indexService = new IndexService();

        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 50));

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Set<QueryableEntry> query = indexService.query(and);

        assertNull(query);
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenNotConditional() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false);
        indexService.addOrGetIndex("salary", false);

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));

        Set<QueryableEntry> query = indexService.query(and);

        assertNotNull(query);
        assertIterableSize(102, query);

    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenQueryContainsAllCondition() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 100));

        fillIndexes(indexService);

        Set<QueryableEntry> query = indexService.query(Predicates.between("salary", 0, 100));

        assertNotNull(query);

        assertIterableSize(202, query);
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenSomeIndexesPredicate() throws Exception {

        IndexService indexService = new IndexService();

        Predicate salaryPredicate = Predicates.between("salary", 0, 100);
        indexService.addOrGetIndex("age", false, salaryPredicate);
        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 1000));

        fillIndexes(indexService);

        Predicate and = Predicates.or(
                salaryPredicate,
                Predicates.between("age", 50, 100),
                Predicates.equal("name", "passiveEmployee500")
        );

        Set<QueryableEntry> query = indexService.query(and);

        assertNull(query);
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenSomeIndexesPartialPredicate() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 1000));

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.between("age", 50, 100)
        );

        Set<QueryableEntry> query = indexService.query(and);

        assertNotNull(query);

        assertIterableSize(102, query);
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenNotIndexAwarePredicate() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("name", false, Predicates.between("salary", 0, 100));

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );

        Set<QueryableEntry> query = indexService.query(and);
        assertNotNull(query);

        assertIterableSize(101, query);
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenNotIndexAwarePredicate() throws Exception {

        IndexService indexService = new IndexService();

        indexService.addOrGetIndex("name", false, Predicates.between("salary", 0, 100));

        fillIndexes(indexService);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );


        Set<QueryableEntry> query = indexService.query(and);
        assertNull(query);
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenNotIndexAwarePredicateIndexNotMatch() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("name", false, Predicates.between("salary", 0, 50));

        fillIndexes(indexService);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = indexService.query(and);
        assertNull(values);

    }

    @Test(timeout = 60000)
    public void testQueryOr_whenNotIndexAwarePredicateIndexNotMatch() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("name", false, Predicates.between("salary", 0, 50));

        fillIndexes(indexService);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = indexService.query(and);
        assertNull(values);
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenSomeIndexesPartialPredicate() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 1000));

        fillIndexes(indexService);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.between("age", 50, 100)
        );
        Collection values = indexService.query(and);
        assertNull(values);
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenIndexesNotMatch() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("salary", 0, 50));

        fillIndexes(indexService);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Collection values = indexService.query(and);
        assertNull(values);
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenNotConditional() throws Exception {

        IndexService indexService = new IndexService();


        indexService.addOrGetIndex("age", false);
        indexService.addOrGetIndex("salary", false);

        fillIndexes(indexService);

        Predicate or = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Collection values = indexService.query(or);
        assertNotNull(values);
        assertIterableSize(2000, values);

    }

    @Test(timeout = 60000)
    public void testQueryOr_whenQueryContainsAllCondition() throws Exception {

        IndexService indexService = new IndexService();
        Predicate or = Predicates.or(Predicates.between("salary", 0, 100), Predicates.equal("active", true));
        indexService.addOrGetIndex("age", false, or);

        fillIndexes(indexService);

        Collection values = indexService.query(or);
        assertNotNull(values);
        assertIterableSize(1101, values);

    }

    @Test(timeout = 60000)
    public void testQuery_whenComplexIndexCondition() throws Exception {

        IndexService indexService = new IndexService();
        Predicate predicate = Predicates.and(Predicates.between("age", 0, 100), Predicates.and(Predicates.between("salary", 0, 40), Predicates.equal("active", true)));
        indexService.addOrGetIndex("age", false, predicate);

        fillIndexes(indexService);

        Collection values = indexService.query(Predicates.and(
                Predicates.or(Predicates.between("salary", 0, 100), Predicates.equal("active", true)),
                predicate,
                Predicates.greaterEqual("age", 10)));
        assertNotNull(values);
        assertIterableSize(31, values);
    }

    @Test(timeout = 60000)
    public void testQuery_whenQueryPartialIndex() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("age", 0, 100));

        fillIndexes(indexService);

        Collection values = indexService.query(Predicates.between("age", 0, 50));
        assertNotNull(values);
        assertIterableSize(102, values);


    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenMultipleSameAttributePredicate() throws Exception {

        IndexService indexService = new IndexService();
        indexService.addOrGetIndex("age", false, Predicates.between("age", 0, 100));

        fillIndexes(indexService);

        Predicate or = Predicates.and(Predicates.between("age", 0, 100), Predicates.notEqual("age", 100));
        Collection values = indexService.query(or);
        assertNotNull(values);

        assertIterableSize(200, values);
    }


}
