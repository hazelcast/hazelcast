package com.hazelcast.replicatedmap;

import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.wan.WanNoDelayReplication;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class HzClusterUtil {

    private Random random = new Random();

    private HazelcastInstanceFactory factory = new HazelcastInstanceFactory();

    private HazelcastInstance[] cluster;
    private Config[] configs;

    private boolean AUTO_RENAME=true;

    public HzClusterUtil(String groupName, int port, int clusterSZ){
        setup( groupName,  port,  clusterSZ);
    }

    private HzClusterUtil(HazelcastInstance[] cluster, Config[] configs){
        this.cluster = cluster;
        this.configs = configs;
    }

    public void setAutoRenameCluster(boolean rename){
        AUTO_RENAME=rename;
    }

    public HazelcastInstance[] getCluster(){
        return cluster;
    }

    public Config[] getConfigs(){
        return configs;
    }

    public void setup(String groupName, int port, int clusterSZ){
        cluster = new HazelcastInstance[clusterSZ];
        configs = new Config[clusterSZ];

        List<String> members = getClusterEndPointsFrom(port, clusterSZ);

        for(int i=0; i<clusterSZ; i++){
            Config config = new Config();
            config.getGroupConfig().setName(groupName);
            config.getNetworkConfig().setPort(port);

            boolean multiCastEnabled=false;
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multiCastEnabled);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multiCastEnabled);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(members);

            configs[i]=config;
        }
    }

    public String getName(){
        return getConfig().getGroupConfig().getName();
    }

    private void setName(String name){
        for(Config c:configs){
            c.getGroupConfig().setName(name);
        }
    }

    private void setMembers(List<String> members){
        for(Config c:configs){
            c.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(members);
        }
    }

    public int getSize(){return cluster.length;}

    public void initCluster(){
        for(int i=0; i<cluster.length; i++){
            cluster[i]= factory.newHazelcastInstance(configs[i]);
        }
    }

    public HazelcastInstance getRandomNode(){
        return cluster[random.nextInt(cluster.length) ];
    }

    public HazelcastInstance getNode(int index){
        return cluster[index];
    }

    public Config getConfig(){
        return getRandomNode().getConfig();
    }

    public void terminateRandomNode(){
        HazelcastInstance node = getRandomNode();
        TestUtil.terminateInstance(node);
    }

    private List<String> getClusterEndPoints(){
        return getClusterEndPoints(this.getConfig(), cluster.length);
    }

    private List<String> getClusterEndPoints(Config toconfig, int count){
        return getClusterEndPointsFrom(toconfig.getNetworkConfig().getPort(), count);
    }

    private List<String> getClusterEndPointsFrom(int port, int count){
        List ends = new ArrayList<String>();

        for(int i=0; i<count; i++){
            ends.add(new String("127.0.0.1:"+port++ ) );
        }
        return ends;
    }

    private WanTargetClusterConfig targetCluster(Config toconfig, int count){
        WanTargetClusterConfig target = new WanTargetClusterConfig();
        target.setGroupName(toconfig.getGroupConfig().getName());
        target.setReplicationImpl(WanNoDelayReplication.class.getName());
        target.setEndpoints(getClusterEndPoints(toconfig, count));
        return target;
    }



    public void replicateTo(HzClusterUtil c, String policy){
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(getName()+"to"+c.getName());
        wanConfig.addTargetClusterConfig(targetCluster(c.getConfig(), c.getSize()));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(wanConfig.getName());
        wanRef.setMergePolicy(policy);

        for(Config config : configs){
            config.addWanReplicationConfig(wanConfig);
            config.getMapConfig("default").setWanReplicationRef(wanRef);
        }
    }


    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        final Node n1 = TestUtil.getNode(h1);
        final Node n2 = TestUtil.getNode(h2);
        n1.clusterService.removeAddress(n2.address);
        n2.clusterService.removeAddress(n1.address);
    }

    private void disConnectCluster(){
        for(int i=0; i<cluster.length/2; i++)
            for(int j=cluster.length/2; j<cluster.length; j++)
                closeConnectionBetween(cluster[i], cluster[j]);
    }


    private void setSplitHalfClusterConfig(){
        List split1 = getClusterEndPoints(getConfig(), cluster.length/2);

        String name = getName();

        for(int i=0; i<cluster.length/2; i++){

            if(AUTO_RENAME){
                cluster[i].getConfig().getGroupConfig().setName(name+"("+1+")");
            }
            cluster[i].getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(split1);
        }

        int port = getConfig().getNetworkConfig().getPort() + cluster.length/2;
        List split2 = getClusterEndPointsFrom(port, cluster.length / 2);

        for(int i=cluster.length/2; i<cluster.length; i++){

            if(AUTO_RENAME){
                cluster[i].getConfig().getGroupConfig().setName(name+"("+2+")");
            }
            cluster[i].getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(split2);
        }
    }

    private HazelcastInstance[] splitClusterArray(){
        HazelcastInstance[] a = Arrays.copyOfRange(cluster, 0, cluster.length/2);
        HazelcastInstance[] b = Arrays.copyOfRange(cluster, cluster.length/2, cluster.length);
        cluster = a;
        return b;
    }

    private Config[] splitConfigArray(){
        Config[] a = Arrays.copyOfRange(configs, 0, configs.length/2);
        Config[] b = Arrays.copyOfRange(configs, configs.length/2, configs.length);
        configs = a;
        return b;
    }


    private void allInstancesReJoin(){
        for(HazelcastInstance i:cluster)
            TestUtil.getNode(i).rejoin();
    }

    public HzClusterUtil splitCluster(){
        setSplitHalfClusterConfig();
        disConnectCluster();

        HzClusterUtil b = new HzClusterUtil(splitClusterArray(), splitConfigArray());

        this.allInstancesReJoin();
        b.allInstancesReJoin();

        return b;
    }


    private void initMergeClustersConfig(HzClusterUtil b){

        String name;
        if(AUTO_RENAME){
            name = this.getName() +"+"+ b.getName();
        }else{
            name = this.getName();
        }

        this.setName(name);
        b.setName(name);

        List<String> all = this.getClusterEndPoints();
        all.addAll(b.getClusterEndPoints());

        this.setMembers(all);
        b.setMembers(all);
    }


    public void mergeCluster(HzClusterUtil b){
        initMergeClustersConfig(b);
        b.allInstancesReJoin();

        cluster = concat(cluster, b.cluster);
        b.cluster=null;

        configs = concat(configs, b.configs);
        b.configs=null;
    }

    public static <T> T[] concat(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }



    public void putMapData(String mapName, int start, int end){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.put(getName()+start, getName()+start);
    }

    public void putAsynctMapData(String mapName, int start, int end){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.putAsync(getName()+ start, getName() + start);
    }

    public void putAsynctMapData(String mapName, int start, int end, int nodeindex){
        HazelcastInstance node = getNode(nodeindex);
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.putAsync(getName() + start, getName() + start);
    }

    public void removeMapData(String mapName, int start, int end){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            m.remove(getName()+start);
    }

    public boolean checkMapKeys(String mapName, int start, int end){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        for(; start<end; start++)
            if(!m.containsKey(getName()+start))
                return false;
        return true;
    }

    private boolean mapDataFrom(String mapName, int start, int end, HzClusterUtil sourceCluster){

        HazelcastInstance node = getRandomNode();

        String sourceGroupName = sourceCluster.getName();

        IMap m = node.getMap(mapName);
        for(; start<end; start++){
            Object v = m.get(sourceGroupName+start);
            if(v==null || !v.equals(sourceGroupName+start))
                return false;
        }
        return true;
    }


    private boolean checkKeysNotIn(String mapName, int start, int end){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        String name = getName();
        for(; start<end; start++)
            if(m.containsKey(name+start))
                return false;
        return true;
    }

    private void assertDataSize(String mapName, int size){
        HazelcastInstance node = getRandomNode();
        IMap m = node.getMap(mapName);
        assertEquals(size, m.size());
    }

    public void assertMapDataFrom(final String mapName, final int start, final int end, final HzClusterUtil sourceCluster){
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(mapDataFrom(mapName, start, end, sourceCluster) );
            }
        });
    }


    public void assertClusterSizeEventually(){
        assertClusterSizeEventually(cluster.length);
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