package com.hazelcast.jca;

import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.executor.impl.ExecutorServiceProxy;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class HazelcastConnectionImplTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private HazelcastConnectionImpl connection;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        ManagedConnectionImpl managedConnection = mock(ManagedConnectionImpl.class);
        when(managedConnection.getHazelcastInstance()).thenReturn(hz);
        connection = new HazelcastConnectionImpl(managedConnection, null);

    }

    @Test
    public void getRingbuffer() {
        Ringbuffer rb = connection.getRingbuffer("ringbuffer");
        assertSame(hz.getRingbuffer("ringbuffer"), rb);
    }

    @Test
    public void getReliableTopic() {
        ITopic topic = connection.getReliableTopic("reliableTopic");
        assertSame(hz.getReliableTopic("reliableTopic"), topic);
    }

    @Test
    public void getTopic() {
        ITopic topic = connection.getTopic("reliableTopic");
        assertSame(hz.getTopic("reliableTopic"), topic);
    }

    @Test
    public void getMap() {
        IMap topic = connection.getMap("map");
        assertSame(hz.getMap("map"), topic);
    }

    @Test
    public void getQueue() {
        IQueue queue = connection.getQueue("queue");
        assertSame(hz.getQueue("queue"), queue);
    }

    @Test
    public void getMultiMap() {
        MultiMap multiMap = connection.getMultiMap("multiMap");
        assertSame(hz.getMultiMap("multiMap"), multiMap);
    }

    @Test
    public void getReplicatedMap() {
        ReplicatedMap replicatedMap = connection.getReplicatedMap("replicatedMap");
        assertSame(hz.getReplicatedMap("replicatedMap"), replicatedMap);
    }

    @Test
    public void getSet() {
        Set set = connection.getSet("set");
        assertSame(hz.getSet("set"), set);
    }

    @Test
    public void getList() {
        IList list = connection.getList("list");
        assertSame(hz.getList("list"), list);
    }

    @Test
    public void getSemaphore() {
        ISemaphore semaphore = connection.getSemaphore("s");
        assertSame(hz.getSemaphore("s"), semaphore);
    }

    @Test
    public void getLock() {
        ILock lock = connection.getLock("lock");
        assertSame(hz.getLock("lock"), lock);
    }

    @Test
    public void getExecutorService() {
        ExecutorService ex = connection.getExecutorService("ex");
        assertSame(hz.getExecutorService("ex"), ex);
    }

    @Test
    public void getAtomicLong() {
        IAtomicLong atomicLong = connection.getAtomicLong("atomicLong");
        assertSame(hz.getAtomicLong("atomicLong"), atomicLong);
    }

    @Test
    public void getIdGenerator() {
        IdGenerator idGenerator = connection.getIdGenerator("id");
        assertSame(hz.getIdGenerator("id"), idGenerator);
    }

    @Test
    public void getDistributedObject() {
        DistributedObject obj = connection.getDistributedObject(MapService.SERVICE_NAME, "id");
        assertSame(hz.getDistributedObject(MapService.SERVICE_NAME, "id"), obj);
    }

    @Test
    @Ignore
    public void getTransactionalQueue() {
        //todo
    }

    @Test
    @Ignore
    public void getTransactionalMap() {
        //todo
    }

    @Test
    @Ignore
    public void getTransactionalList() {
        //todo
    }

    @Test
    @Ignore
    public void getTransactionalSet() {
        //todo
    }

    @Test
    public void getAtomicReference() {
        IAtomicReference ref = connection.getAtomicReference("ref");
        assertSame(hz.getAtomicReference("ref"), ref);
    }

    @Test
    public void getName() {
        String name = connection.getName();
        assertSame(name, hz.getName());
    }

    @Test
    public void getConfig() {
        Config config = connection.getConfig();
        assertSame(config, hz.getConfig());
    }

    @Test
    public void getJobTracker() {
        JobTracker jobTracker = connection.getJobTracker("jobTracker");
        assertSame(jobTracker, hz.getJobTracker("jobTracker"));
    }

    @Test
    public void getCluster() {
        Cluster cluster = connection.getCluster();
        assertNotSame(cluster, hz.getCluster());
    }

    @Test
    public void getQuorumService() {
        QuorumService quorumService = connection.getQuorumService();
        assertSame(quorumService, hz.getQuorumService());
    }

    @Test
    public void getClientService() {
        ClientService clientService = connection.getClientService();
        assertNotSame(clientService, hz.getClientService());
    }

    @Test
    public void getLoggingService() {
        LoggingService loggingService = connection.getLoggingService();
        assertSame(loggingService, hz.getLoggingService());
    }

    @Test
    public void getUserContext() {
        Map userContext = connection.getUserContext();
        assertSame(userContext, hz.getUserContext());
    }

    @Test
    public void getPartitionService() {
        PartitionService partitionService = connection.getPartitionService();
        assertSame(partitionService, hz.getPartitionService());
    }

    @Test
    public void getLocalEndpoint() {
        Endpoint endpoint = connection.getLocalEndpoint();
        assertSame(endpoint, hz.getLocalEndpoint());
    }

    @Test
    public void getXAResource() {
        XAResource resource = connection.getXAResource();
        assertNull(resource);
    }
}
