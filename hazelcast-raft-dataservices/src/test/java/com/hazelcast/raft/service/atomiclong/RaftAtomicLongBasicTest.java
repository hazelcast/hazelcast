package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.config.raft.RaftConfig.DEFAULT_RAFT_GROUP_NAME;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private IAtomicLong atomicLong;
    private String name = "id";

    @Before
    public void setup() {
        instances = createInstances();
        atomicLong = createAtomicLong(name);
        assertNotNull(atomicLong);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(5, 3, 2);
    }

    protected IAtomicLong createAtomicLong(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftAtomicLongService.SERVICE_NAME, name);
    }

    @Test
    public void testSet() {
        atomicLong.set(271);
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGet() {
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testDecrementAndGet() {
        assertEquals(-1, atomicLong.decrementAndGet());
        assertEquals(-2, atomicLong.decrementAndGet());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndSet() {
        assertEquals(0, atomicLong.getAndSet(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testAddAndGet() {
        assertEquals(271, atomicLong.addAndGet(271));
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, atomicLong.getAndAdd(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenSuccess() {
        assertTrue(atomicLong.compareAndSet(0, 271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenNotSuccess() {
        assertFalse(atomicLong.compareAndSet(172, 0));
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testAlter() {
        atomicLong.set(2);

        atomicLong.alter(new MultiplyByTwo());

        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGet() {
        atomicLong.set(2);

        long result = atomicLong.alterAndGet(new MultiplyByTwo());

        assertEquals(4, result);
    }

    @Test
    public void testGetAndAlter() {
        atomicLong.set(2);

        long result = atomicLong.getAndAlter(new MultiplyByTwo());

        assertEquals(2, result);
        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Void> f = atomicLong.alterAsync(new MultiplyByTwo());
        f.get();

        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGetAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.alterAndGetAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(4, result);
    }

    @Test
    public void testGetAndAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.getAndAlterAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(2, result);
        assertEquals(4, atomicLong.get());
    }

    @Test
    public void testApply() {
        atomicLong.set(2);

        long result = atomicLong.apply(new MultiplyByTwo());

        assertEquals(4, result);
        assertEquals(2, atomicLong.get());
    }

    @Test
    public void testApplyAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        Future<Long> f = atomicLong.applyAsync(new MultiplyByTwo());
        long result = f.get();

        assertEquals(4, result);
        assertEquals(2, atomicLong.get());
    }

    @Test
    public void testLocalGet_withLeaderLocalPolicy() {
        atomicLong.set(3);

        RaftAtomicLongProxy atomicLongProxy = (RaftAtomicLongProxy) atomicLong;
        long v = atomicLongProxy.localGet(QueryPolicy.LEADER_LOCAL);

        assertEquals(3, v);
    }

    @Test
    public void testLocalGet_withAnyLocalPolicy() {
        atomicLong.set(3);

        RaftAtomicLongProxy atomicLongProxy = (RaftAtomicLongProxy) atomicLong;
        long v = atomicLongProxy.localGet(QueryPolicy.ANY_LOCAL);

        assertEquals(3, v);
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicLong atomicLong = createAtomicLong(randomName());
        assertEquals(DEFAULT_RAFT_GROUP_NAME, getGroupId(atomicLong).name());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        atomicLong.destroy();
        atomicLong.incrementAndGet();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        atomicLong.destroy();

        atomicLong = createAtomicLong(name);
        atomicLong.incrementAndGet();
    }

    @Test
    public void testMultipleDestroy() {
        atomicLong.destroy();
        atomicLong.destroy();
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicLong.destroy();

        final RaftGroupId groupId = getGroupId(atomicLong);
        getRaftInvocationManager(instances[0]).triggerDestroy(groupId).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                atomicLong = createAtomicLong(name);
                RaftGroupId newGroupId = getGroupId(atomicLong);
                assertNotEquals(groupId, newGroupId);
            }
        });

        atomicLong.incrementAndGet();
    }

    protected RaftGroupId getGroupId(IAtomicLong atomicLong) {
        return ((RaftAtomicLongProxy) atomicLong).getGroupId();
    }

    public static class MultiplyByTwo implements IFunction<Long, Long> {

        @Override
        public Long apply(Long input) {
            return input * 2;
        }
    }
}
