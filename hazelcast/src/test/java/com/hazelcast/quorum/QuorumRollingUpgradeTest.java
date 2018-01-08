package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.ExecRunnable.runnable;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;

/**
 * Quorum test proving that newly supported quorum-aware structures do not respect quorum below version 3.10
 */
@Category({QuickTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class QuorumRollingUpgradeTest extends AbstractQuorumTest {

    private static final int NO_QUORUM_CLUSTER = 3;

    private static final QuorumType TYPE = QuorumType.WRITE;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, "3.9");
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        shutdownTestEnvironment();
    }

    public TransactionContext newTransactionContext(int index, TransactionOptions options) {
        return cluster.instance[index].newTransactionContext(options);
    }

    //
    // NEWLY SUPPORTED QUORUM-AWARE STRUCTURES DO NOT HONOR QUORUM BELOW 3.10
    //
    @Test
    public void atomicLong() {
        along(NO_QUORUM_CLUSTER, TYPE).addAndGet(1);
    }

    @Test
    public void atomicReference() throws Exception {
        aref(NO_QUORUM_CLUSTER, TYPE).getAndAlterAsync(function()).get();
    }

    @Test
    public void cardinalityEstimator() {
        estimator(NO_QUORUM_CLUSTER, TYPE).add(1);
    }

    @Test
    public void countDownLatch() {
        latch(NO_QUORUM_CLUSTER, TYPE).countDown();
    }

    @Test
    public void durableExecutor() {
        durableExec(NO_QUORUM_CLUSTER, TYPE).execute(runnable());
    }

    @Test
    public void executor() {
        exec(NO_QUORUM_CLUSTER, TYPE).execute(runnable());
    }

    @Test
    public void list() {
        list(NO_QUORUM_CLUSTER, TYPE).add("foo");
    }

    @Test
    public void txList() {
        TransactionOptions options = TransactionOptions.getDefault();
        options.setTransactionType(ONE_PHASE);

        TransactionContext transactionContext = newTransactionContext(NO_QUORUM_CLUSTER, options);
        transactionContext.beginTransaction();
        TransactionalList<Object> list = transactionContext.getList(LIST_NAME + TYPE.name());
        list.add("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void multiMap() {
        multimap(NO_QUORUM_CLUSTER, TYPE).put("foo", "bar");
    }

    @Test
    public void txMultiMap() {
        TransactionOptions options = TransactionOptions.getDefault();
        options.setTransactionType(ONE_PHASE);
        TransactionContext transactionContext = newTransactionContext(NO_QUORUM_CLUSTER, options);

        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + TYPE.name());
        map.put("12NO_QUORUM_SIDE", "456");
        transactionContext.commitTransaction();
    }

    @Test
    public void replicatedMap() {
        replmap(NO_QUORUM_CLUSTER, TYPE).put("foo", "bar");
    }

    @Test
    public void ringbuffer() {
        ring(NO_QUORUM_CLUSTER, TYPE).add("123");
    }

    @Test
    public void scheduledExecutor() {
        exec(NO_QUORUM_CLUSTER, TYPE).execute(runnable());
    }

    @Test
    public void set() {
        set(NO_QUORUM_CLUSTER, TYPE).add("foo");
    }

    @Test
    public void txSet() {
        TransactionOptions options = TransactionOptions.getDefault();
        options.setTransactionType(ONE_PHASE);
        TransactionContext transactionContext = newTransactionContext(NO_QUORUM_CLUSTER, options);

        transactionContext.beginTransaction();
        TransactionalSet<Object> list = transactionContext.getSet(SET_NAME + TYPE.name());
        list.add("foo");
        transactionContext.commitTransaction();
    }

    //
    // PROOF THAT "OLD" QUORUM-AWARE STRUCTURES HONOR QUORUM BELOW 3.10
    //
    @Test(expected = QuorumException.class)
    public void map() {
        map(NO_QUORUM_CLUSTER, TYPE).put("foo", "bar");
    }

}