package com.hazelcast.test.modularhelpers;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;

import java.util.*;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class SimpleClusterUtil {

    private Random random = new Random();

    private HazelcastInstanceFactory factory = new HazelcastInstanceFactory();

    private int minClusterSize=-1;
    private int maxClusterSize=100;
    private int initialClusterSize=0;
    private List<HazelcastInstance> cluster;
    private Config config = new Config();

    public SimpleClusterUtil(int clusterSZ){
        initialClusterSize = clusterSZ;
        cluster = Collections.synchronizedList( new ArrayList<HazelcastInstance>(initialClusterSize) );
    }

    public void initCluster(){
        for(int i=0; i<initialClusterSize; i++){
            cluster.add( factory.newHazelcastInstance( config ) );
        }
    }

    public void setMinClusterSize(int min){
        minClusterSize=min;
    }
    public void setMaxClusterSize(int max){
        maxClusterSize=max;
    }

    public boolean isMinSize(){
        return cluster.size() <= minClusterSize;
    }
    public boolean isMaxSize(){
        return cluster.size() >= maxClusterSize;
    }

    public int getSize(){ return cluster.size(); }

    public String getName(){ return config.getGroupConfig().getName(); }

    public HazelcastInstance getNode(int index){
        return cluster.get(index);
    }

    public HazelcastInstance getRandomNode(){
        return getNode(random.nextInt(cluster.size()));
    }

    public void terminateRandomNode(){
        HazelcastInstance node = getRandomNode();
        cluster.remove(node);

        node.getLifecycleService().terminate();
    }

    public void shutDownRandomNode(){
        HazelcastInstance node = getRandomNode();
        cluster.remove(node);

        node.getLifecycleService().shutdown();
    }

    public void shutDown() {

        for (HazelcastInstance hz : cluster) {
            try {
                hz.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void addNode(){
        cluster.add( factory.newHazelcastInstance( config ) );
    }

    public Config getConfig(){
        return config;
    }

    public void setConfig(Config config){
        this.config = config;
    }

    public void assertClusterSizeEventually(final int sz){
        assertTrueEventually(new AssertTask() {
            public void run() {
                for(HazelcastInstance i : cluster)
                    assertEquals(sz, i.getCluster().getMembers().size());
            }
        });
    }
}