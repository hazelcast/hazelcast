package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.jruby.util.Random;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.*;

/**
 * Created by danny on 12/10/13.
 */


@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class WanReplicationTest {

    private HazelcastInstanceFactory factory = new HazelcastInstanceFactory();

    private HazelcastInstance[] clusterA = new HazelcastInstance[2];
    private HazelcastInstance[] clusterB = new HazelcastInstance[2];
    private HazelcastInstance[] clusterC = new HazelcastInstance[2];

    private Config configA;
    private Config configB;
    private Config configC;


    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();

    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }


    @Before
    public void setup() throws Exception {
        configA = new Config();
        configA.getGroupConfig().setName("A");
        configA.getNetworkConfig().setPort(5701);

        configB = new Config();
        configB.getGroupConfig().setName("B");
        configB.getNetworkConfig().setPort(5801);

        configC = new Config();
        configC.getGroupConfig().setName("C");
        configC.getNetworkConfig().setPort(5901);
    }


    private void initCluster(HazelcastInstance[] cluster, Config config){
        for(int i=0; i<cluster.length; i++){
            cluster[i]= factory.newHazelcastInstance(config);
        }
    }


    private void initClusterA(){
        initCluster(clusterA, configA);
    }
    private void initClusterB(){
        initCluster(clusterB, configB);
    }
    private void initClusterC(){
        initCluster(clusterC, configC);
    }
    private void initAllClusters(){
        initClusterA();
        initClusterB();
        initClusterC();
    }


    private HazelcastInstance getNode(HazelcastInstance[] cluster){
        return cluster[Random.N % cluster.length ];
    }


    private List getClusterEndPoints(Config config, int count){
        List ends = new ArrayList<String>();

        int port = config.getNetworkConfig().getPort();

        for(int i=0; i<count; i++){
            ends.add(new String("127.0.0.1:"+port++ ) );
        }
        return ends;
    }

    private WanTargetClusterConfig targetCluster(Config config, int count){
        WanTargetClusterConfig target = new WanTargetClusterConfig();
        target.setGroupName(config.getGroupConfig().getName());
        target.setReplicationImpl(WanNoDelayReplication.class.getName());
        target.setEndpoints(getClusterEndPoints(config, count));
        return target;
    }



    private void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy){
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(setupName);
        wanConfig.addTargetClusterConfig(targetCluster(toConfig, clusterSz));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getMapConfig("default").setWanReplicationRef(wanRef);
    }

    private void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end){
        HazelcastInstance node = getNode(cluster);
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.put(start, node.getConfig().getGroupConfig().getName()+start);
    }

    private void removeDataIn(HazelcastInstance[] cluster, String mapName, int start, int end){
        HazelcastInstance node = getNode(cluster);
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.remove(start);
    }

    private boolean checkKeysIn(HazelcastInstance[] cluster, String mapName, int start, int end){
        HazelcastInstance node = getNode(cluster);
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            if(!m.containsKey(start))
                return false;
        return true;
    }

    private boolean checkDataInFrom(HazelcastInstance[] targetCluster, String mapName, int start, int end, HazelcastInstance[] sourceCluster){
        HazelcastInstance node = getNode(targetCluster);

        String sourceGroupName = getNode(sourceCluster).getConfig().getGroupConfig().getName();

        IMap m = node.getMap(mapName);
        for(; start<end; start++){
            Object v = m.get(start);
            if(v==null || !v.equals(sourceGroupName+start))
                return false;
        }
        return true;
    }


    private boolean checkKeysNotIn(HazelcastInstance[] cluster, String mapName, int start, int end){
        HazelcastInstance node = getNode(cluster);
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            if(m.containsKey(start))
                return false;
        return true;
    }

    private void assertDataSize(final HazelcastInstance[] cluster, String mapName, int size){
        HazelcastInstance node = getNode(cluster);
        IMap m = node.getMap(mapName);
        assertEquals(size, m.size());
    }


    private void assertKeysIn(final HazelcastInstance[] cluster, final String mapName, final int start, final int end){
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(checkKeysIn(cluster, mapName, start, end) );
            }
        });
    }

    private void assertDataInFrom(final HazelcastInstance[] cluster, final String mapName, final int start, final int end, final HazelcastInstance[] sourceCluster){
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(checkDataInFrom(cluster, mapName, start, end, sourceCluster) );
            }
        });
    }

    private void assertKeysNotIn(final HazelcastInstance[] cluster, final String mapName, final int start, final int end){
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(checkKeysNotIn(cluster, mapName, start, end) );
            }
        });
    }




    // V topo config 1 passive replicar, 2 producers
    @Test
    public void VTopo_1passiveReplicar_2producers_Test_PassThroughMergePolicy(){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0,    1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterC, "map", 1000, 2000, clusterB);

        createDataIn(clusterB, "map", 0, 1);
        assertDataInFrom(clusterC, "map", 0, 1, clusterB);

        removeDataIn(clusterA, "map", 0, 500);
        removeDataIn(clusterB, "map", 1500, 2000);

        assertKeysNotIn(clusterC, "map", 0, 500);
        assertKeysNotIn(clusterC, "map", 1500, 2000);

        assertKeysIn(clusterC, "map", 500, 1500);

        removeDataIn(clusterA, "map", 500, 1000);
        removeDataIn(clusterB, "map", 1000, 1500);

        assertKeysNotIn(clusterC, "map", 0, 2000);
        assertDataSize(clusterC, "map", 0);
    }


    @Test
    public void Vtopo_TTL_Replication_Issue254(){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());

        configA.getMapConfig("default").setTimeToLiveSeconds(2);
        configB.getMapConfig("default").setTimeToLiveSeconds(2);
        configC.getMapConfig("default").setTimeToLiveSeconds(2);


        initAllClusters();

        createDataIn(clusterA, "map", 0,  10);
        assertDataInFrom(clusterC, "map", 0,  10, clusterA);

        createDataIn(clusterB, "map", 10, 20);
        assertDataInFrom(clusterC, "map", 10, 20, clusterB);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        assertKeysNotIn(clusterA, "map",  0, 10);
        assertKeysNotIn(clusterB, "map", 10, 20);
        assertKeysNotIn(clusterC, "map",  0, 20);
    }



    @Ignore("Issue #1371 this topology requested hear https://groups.google.com/forum/#!msg/hazelcast/73jJo9W_v4A/5obqKMDQAnoJ")
    @Test
    public void VTopo_1activeActiveReplicar_2producers_Test_PassThroughMergePolicy(){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());

        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configB, clusterB.length, "ctob", PassThroughMergePolicy.class.getName());

        initAllClusters();

        printAllReplicarConfig();

        createDataIn(clusterA, "map", 0,    1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterC, "map", 1000, 2000, clusterB);

        assertDataInFrom(clusterA, "map", 1000, 2000, clusterB);
        assertDataInFrom(clusterB, "map", 0,    1000, clusterA);
    }


    @Test
    public void VTopo_1passiveReplicar_2producers_Test_PutIfAbsentMapMergePolicy(){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PutIfAbsentMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PutIfAbsentMapMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0,    1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterC, "map", 1000, 2000, clusterB);

        createDataIn(clusterB, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        assertDataSize(clusterC, "map", 2000);

        removeDataIn(clusterA, "map", 0, 1000);
        removeDataIn(clusterB, "map", 1000, 2000);

        assertKeysNotIn(clusterC, "map", 0, 2000);
        assertDataSize(clusterC, "map", 0);
    }


    @Test
    public void VTopo_1passiveReplicar_2producers_Test_LatestUpdateMapMergePolicy (){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", LatestUpdateMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", LatestUpdateMapMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterB);

        assertDataSize(clusterC, "map", 1000);

        removeDataIn(clusterA, "map", 0, 500);
        assertKeysNotIn(clusterC, "map", 0, 500);

        removeDataIn(clusterB, "map", 500, 1000);
        assertKeysNotIn(clusterC, "map", 500, 1000);

        assertDataSize(clusterC, "map", 0);
    }



    @Ignore("Issue #1373  this test passes when run in isolation")
    @Test
    public void VTopo_1passiveReplicar_2producers_Test_HigherHitsMapMergePolicy(){

        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsMapMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);
        createDataIn(clusterB, "map", 0, 1000);
        createDataIn(clusterB, "map", 0, 1000);


        assertDataInFrom(clusterC, "map", 0, 1000, clusterB);
    }



    @Ignore("Issue #1368 multi replicar topology cluster A replicates to B and C")
    @Test
    public void VTopo_2passiveReplicar_1producer_Test(){

        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        initAllClusters();


        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterC, "map", 0, 1000);

        removeDataIn(clusterA, "map", 0, 1000);

        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterC, "map", 0, 1000);

        assertDataSize(clusterB, "map", 0);
        assertDataSize(clusterC, "map", 0);
    }



    @Test
    public void linkTopo_ActiveActiveReplication_Test(){

        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        initClusterA();
        initClusterB();


        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 1000, 2000);
        assertDataInFrom(clusterA, "map", 1000, 2000, clusterB);

        removeDataIn(clusterA, "map", 1500, 2000);
        assertKeysNotIn(clusterB, "map", 1500, 2000);

        removeDataIn(clusterB, "map", 0, 500);
        assertKeysNotIn(clusterA, "map", 0, 500);

        assertKeysIn(clusterA, "map", 500, 1500);
        assertKeysIn(clusterB, "map", 500, 1500);

        assertDataSize(clusterA, "map", 1000);
        assertDataSize(clusterB, "map", 1000);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_Threading_Test() throws InterruptedException, BrokenBarrierException {

        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        initClusterA();
        initClusterB();


        CyclicBarrier gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();

        assertDataInFrom(clusterB, "map", 0,     500, clusterA);
        assertDataInFrom(clusterA, "map", 1000, 1500, clusterB);
        assertKeysIn(clusterA, "map", 500, 1000);


        gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            public void go() {
                removeDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            public void go() {
                removeDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();


        assertKeysNotIn(clusterA, "map", 0, 1500);
        assertKeysNotIn(clusterB, "map", 0, 1500);

        assertDataSize(clusterA, "map", 0);
        assertDataSize(clusterB, "map", 0);
    }



    @Test
    public void linkTopo_ActiveActiveReplication_2clusters_Test_HigherHitsMapMergePolicy(){

        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        initClusterA();
        initClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 500);
        assertDataInFrom(clusterA, "map", 0, 500, clusterB);
    }

    @Ignore("Issue #1372  is a chain of replicars a valid topology")
    @Test
    public void chainTopo_2passiveReplicars_1producer(){

        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertDataSize(clusterB, "map", 1000);

        assertKeysIn(clusterC, "map", 0, 1000);
        assertDataSize(clusterC, "map", 1000);
    }



    @Ignore("Issue #1372 is a ring topology valid")
    @Test
    public void replicationRing(){

        setupReplicateFrom(configA, configB, clusterB.length,"atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length,"btoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configA, clusterA.length,"ctoa", PassThroughMergePolicy.class.getName());
        initAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertDataSize(clusterB, "map", 1000);

        assertKeysIn(clusterC, "map", 0, 1000);
        assertDataSize(clusterC, "map", 1000);
    }


    private void printReplicaConfig(Config c){

        Map m = c.getWanReplicationConfigs();
        Set<Entry> s = m.entrySet();
        for(Entry e : s ){
            System.out.println(e.getKey() + " ==> " + e.getValue());
        }
    }

    private void printAllReplicarConfig(){
        System.out.println();
        System.out.println("==configA==");
        printReplicaConfig(configA);
        System.out.println("==configB==");
        printReplicaConfig(configB);
        System.out.println("==configC==");
        printReplicaConfig(configC);
        System.out.println();
    }


    abstract public class GatedThread extends Thread{
        private final CyclicBarrier gate;

        public GatedThread(CyclicBarrier gate){
            this.gate = gate;
        }

        public void run(){
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        abstract public void go();
    }

    void startGatedThread(GatedThread t){
        t.start();
    }


    public static void assertTrueEventually(AssertTask task) {
        AssertionError error = null;
        for (int k = 0; k < 60; k++) {
            try {
                task.run();
                return;
            } catch (AssertionError e) {
                error = e;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw error;
    }
}