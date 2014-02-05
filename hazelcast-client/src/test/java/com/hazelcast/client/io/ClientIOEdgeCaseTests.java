package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.modularhelpers.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientIOEdgeCaseTests {

    public ClusterClientDefaultConfig scenario = null;


    public class ClusterClientDefaultConfig extends TestScenarios {

        public ActionBlocks clientOps = new ActionBlocks("1", "A");
        public ActionBlocks nodeOps = new ActionBlocks("1", "B");

        public HazelcastInstance client=null;

        public void beforeScenario(){

            if(client!=null && client.getLifecycleService().isRunning()){
                client.getLifecycleService().shutdown();
            }
            Hazelcast.shutdownAll();

            cluster = new SimpleClusterUtil("A", 4);
            cluster.initCluster();

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));

            client = HazelcastClient.newHazelcastClient(clientConfig);

            clientOps.setHzInstance(client);
            nodeOps.setHzInstance(cluster.getRandomNode());
        }

        public void afterScenario(){ }  //not shutting down the cluster hear, as we want to use it for our asserts
    }


    public ClientIOEdgeCaseTests(){
        scenario = new ClusterClientDefaultConfig();
    }

    @Before
    public void init(){
        scenario.beforeScenario();
    }


    @Test
    public void clientMapPut_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.clientOps.mapPut);

        assertEquals(scenario.terminate3.maxIterations, scenario.clientOps.mapPut.map.size());
    }


    @Test
    public void clientListPut_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.clientOps.listAdd);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(scenario.terminate3.maxIterations, scenario.clientOps.listAdd.list.size());
            }
        });
    }



    @Test
    @Category(ProblematicTest.class) //replicated map operations are Async, so this test is unlikley to work ?
    public void clientRepMapPut_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.clientOps.repMapPut);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {

                System.out.println(scenario.clientOps.repMapPut.map.size());
                assertEquals(scenario.terminate3.maxIterations, scenario.clientOps.repMapPut.map.size());
            }
        });
    }


    @Test
    public void clientPuttingOperations_HappyScenario(){
        scenario.happy.run(scenario.clientOps.getAllPutActions());

        assertTrueEventualy_intResult_fromActions(scenario.happy, scenario.clientOps.getAllPutActions(), scenario.happy.maxIterations);
    }

    @Test
    public void clientPuttingOperations_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.clientOps.getAllPutActions());
        assertTrueEventualy_intResult_fromActions(scenario.happy, scenario.clientOps.getAllPutActions(), scenario.happy.maxIterations);
    }


    @Test
    public void nodeExecutorCallCount_Terminate1NodeScenario(){
        scenario.terminate1.run(scenario.nodeOps.executeOnKeyOwner);

        assertEquals(scenario.terminate1.maxIterations, scenario.nodeOps.executeOnKeyOwner.totalCalls);
    }

    @Test
    public void clientExecutorCallCount_Terminate1NodeScenario(){
        scenario.terminate1.run(scenario.clientOps.executeOnKeyOwner);

        assertEquals(scenario.terminate1.maxIterations, scenario.clientOps.executeOnKeyOwner.totalCalls);
    }

    @Test
    public void nodeExecutorCallCount_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.nodeOps.executeOnKeyOwner);

        assertEquals(scenario.terminate3.maxIterations, scenario.nodeOps.executeOnKeyOwner.totalCalls);
    }

    @Test
    public void clientExecutorCallCount_Terminate3NodeScenario(){
        scenario.terminate3.run(scenario.clientOps.executeOnKeyOwner);

        assertEquals(scenario.terminate3.maxIterations, scenario.clientOps.executeOnKeyOwner.totalCalls);
    }


    @Test
    public void mapPutsGetsRemoves_With3NodeTerminate(){
        scenario.terminate3.run(scenario.clientOps.mapPut, scenario.clientOps.mapGet, scenario.clientOps.mapRemove);
    }

    @Test
    public void mapSyncPuts_AsyncGetsRemoves_With3NodeTerminate(){
        scenario.terminate3.run(scenario.clientOps.mapPut, scenario.clientOps.mapGetAsync, scenario.clientOps.mapRemoveAsync);
    }


    protected void assertTrueEventualy_intResult_fromActions(final Scenario scenario, final Action[] actions, final int expected){

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assert_intResult_fromActions(scenario, actions, expected);
            }
        });

    }


    protected void assert_intResult_fromActions(Scenario scenario, Action[] actions, int expected){
        boolean error=false;
        for(Action a : actions){
            try{
                assertEquals(scenario.scenarioName +" "+a.actionName, expected, a.intResult());
            } catch (AssertionError e) {
                e.printStackTrace();
                error=true;
            }
        }
        if(error){throw new AssertionError("error(s) found see console for stack trace");}
    }
}
