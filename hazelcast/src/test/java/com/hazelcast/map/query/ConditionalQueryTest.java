package com.hazelcast.map.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.IndexStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ConditionalQueryTest extends HazelcastTestSupport {

    private void fillMap(IMap imap) {
        for (int i = 0; i < 1000; i++) {
            SampleObjects.Employee employee = new SampleObjects.Employee("activeEmployee" + i, i, true, Double.valueOf(i));
            imap.put("activeEmployee" + i, employee);
        }

        for (int i = 0; i < 1000; i++) {
            SampleObjects.Employee employee = new SampleObjects.Employee("passiveEmployee" + i, i, false, Double.valueOf(i));
            imap.put("passiveEmployee" + i, employee);
        }
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenSomeIndexesPredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        Predicate salaryPredicate = Predicates.between("salary", 0, 100);
        imap.addIndex("age", false, salaryPredicate);
        imap.addIndex("age", false);

        fillMap(imap);

        Predicate and = Predicates.and(
                salaryPredicate,
                Predicates.greaterEqual("age", 50),
                Predicates.notEqual("name", "passiveEmployee50"));

        Collection values = imap.values(and);
        assertEquals(101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(2, indexStats.size());
        for (IndexStats indexStat : indexStats) {
            Predicate predicate = indexStat.getPredicate();
            if (predicate != null && predicate.equals(salaryPredicate)) {
                assertEquals(1, indexStat.getUsageCount());
            } else {
                assertEquals(0, indexStat.getUsageCount());
            }
        }
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenIndexesNotMatch() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, Predicates.between("salary", 0, 50));

        fillMap(imap);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Collection values = imap.values(and);

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(102, values.size());

        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenNotConditional() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false);
        imap.addIndex("salary", false);

        fillMap(imap);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));

        Collection values = imap.entrySet(and);
        assertEquals(102, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(2, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
        assertEquals(1, indexStats.get(1).getUsageCount());

    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenQueryContainsAllCondition() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, Predicates.between("salary", 0, 100));

        fillMap(imap);

        Predicate and = Predicates.and(Predicates.between("salary", 0, 100));
        Collection values = imap.values(and);

        assertEquals(202, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenSomeIndexesPredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        Predicate salaryPredicate = Predicates.between("salary", 0, 100);
        imap.addIndex("age", false, salaryPredicate);
        imap.addIndex("age", false, Predicates.between("salary", 0, 1000));

        fillMap(imap);

        Predicate and = Predicates.or(
                salaryPredicate,
                Predicates.between("age", 50, 100),
                Predicates.equal("name", "passiveEmployee500")
        );
        Collection values = imap.values(and);

        assertEquals(203, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(2, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
        assertEquals(0, indexStats.get(1).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenSomeIndexesPartialPredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, Predicates.between("salary", 0, 1000));

        fillMap(imap);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.between("age", 50, 100)
        );
        Collection values = imap.values(and);

        assertEquals(102, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenNotIndexAwarePredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("name", false, Predicates.between("salary", 0, 100));

        fillMap(imap);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = imap.values(and);

        assertEquals(101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenNotIndexAwarePredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("name", false, Predicates.between("salary", 0, 100));

        fillMap(imap);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = imap.values(and);

        assertEquals(1101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryAnd_whenNotIndexAwarePredicateIndexNotMatch() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("name", false, Predicates.between("salary", 0, 50));

        fillMap(imap);

        Predicate and = Predicates.and(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = imap.values(and);

        assertEquals(101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQueryOr_whenNotIndexAwarePredicateIndexNotMatch() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("name", false, Predicates.between("salary", 0, 50));

        fillMap(imap);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.like("name", "passiveEmployee%")
        );
        Collection values = imap.values(and);

        assertEquals(1101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenSomeIndexesPartialPredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, Predicates.between("salary", 0, 1000));

        fillMap(imap);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.between("age", 50, 100)
        );
        Collection values = imap.values(and);

        assertEquals(202, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());
    }


    @Test(timeout = 60000)
    public void testQueryOr_whenIndexesNotMatch() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, Predicates.between("salary", 0, 50));

        fillMap(imap);

        Predicate and = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Collection values = imap.values(and);

        assertEquals(2000, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(0, indexStats.get(0).getUsageCount());

    }


    @Test(timeout = 60000)
    public void testQueryOr_whenNotConditional() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false);
        imap.addIndex("salary", false);

        fillMap(imap);

        Predicate or = Predicates.or(
                Predicates.between("salary", 0, 100),
                Predicates.greaterEqual("age", 50));
        Collection values = imap.entrySet(or);
        assertEquals(2000, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(2, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
        assertEquals(1, indexStats.get(1).getUsageCount());

    }

    @Test(timeout = 60000)
    public void testQueryOr_whenQueryContainsAllCondition() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        Predicate or = Predicates.or(Predicates.between("salary", 0, 100), Predicates.equal("active", true));
        imap.addIndex("age", false, or);

        fillMap(imap);

        Collection values = imap.values(or);
        assertEquals(1101, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQuery_whenComplexIndexCondition() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        Predicate predicate = Predicates.and(Predicates.between("age", 0, 100), Predicates.and(Predicates.between("salary", 0, 40), Predicates.equal("active", true)));

        imap.addIndex("age", false, predicate);

        fillMap(imap);

        Collection values = imap.values(Predicates.and(
                Predicates.or(Predicates.between("salary", 0, 100), Predicates.equal("active", true)),
                predicate,
                Predicates.greaterEqual("age", 10)));
        assertEquals(31, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQuery_whenQueryPartialIndex() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());

        imap.addIndex("age", false, Predicates.between("age", 0, 100));

        fillMap(imap);

        Collection values = imap.values(Predicates.between("age", 0, 50));
        assertEquals(102, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }

    @Test(timeout = 60000)
    public void testQueryAnd_whenMultipleSameAttributePredicate() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());

        imap.addIndex("age", false, Predicates.between("age", 0, 100));

        fillMap(imap);

        Predicate or = Predicates.and(Predicates.between("age", 0, 100), Predicates.notEqual("age", 100));
        Collection values = imap.values(or);
        assertEquals(200, values.size());

        List<IndexStats> indexStats = imap.getLocalMapStats().getIndexStats();
        assertEquals(1, indexStats.size());
        assertEquals(1, indexStats.get(0).getUsageCount());
    }

    // todo: what should we do when index predicate can't be applied to the entries?
    @Test(timeout = 60000, expected = ClassCastException.class)
    public void testQuery_whenIllegalIndexCondition() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap imap = h1.getMap(randomString());
        Predicate predicate = Predicates.regex("active", ".*");

        imap.addIndex("age", false, predicate);

        fillMap(imap);
    }

    @Test(timeout = 60000)
    public void testAddIndex_whenPredicateIsNull() throws Exception {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap imap = h1.getMap(randomString());
        imap.addIndex("age", false, null);
    }
}
