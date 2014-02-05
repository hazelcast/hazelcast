package com.hazelcast.test.modularhelpers;

import java.util.Random;


//this class contains a number of different scenarios, that could happen to cluster of nodes, while they are exicuteing some instrucionts
//this class is abstract, so that a sub class can decide on the configuration of the cluster, before running each test
//a sub class will decide what to do after a test is run, close the cluster , get more data ect...
public abstract class TestScenarios {

    private Random random = new Random();

    //the cluster of HazelCastInastances we can play with while the scanario is running
    protected SimpleClusterUtil cluster;

    public SimpleClusterUtil getCluster(){return cluster;}

    //helper to call all the instructions
    private void InvokeActions(int stepCount, Action... actions){
        for(Action a : actions)
            a.call(stepCount);
    }


    //all the scenarios below will extend this
    protected abstract class ParalelActions extends ParalelActionScenario {
        public ParalelActions(String name){super(name);}
        public void before(){
            System.out.println(this.scenarioName+" scenario ===>>> Start");
        }
        public void after(){
            System.out.println(this.scenarioName+" scenario ===>>> End");
        }
    }


    //the happy test, where nothings bad happens.
    //we call all the actions with the step number for every step in the scenario
    public final ParalelActionScenario happy = new ParalelActions("happy") {
        public void step(int stepCount, Action... actions) {
            InvokeActions(stepCount, actions);
        }
    };

    //in this scenario we a termination one random node from our cluster, after a quarter of the steps have been done
    public final ParalelActionScenario terminate1 = new ParalelActions("terminate1") {
        public void step(int stepCount, Action... actions) {
            InvokeActions(stepCount, actions);

            if (stepCount == this.maxIterations/4 * 1)
                cluster.terminateRandomNode();
        }
    };

    //terminate 3 nodes at quarter intervals while the scenario is running
    //so to use this corectly the subclass should have a cluster with at least 3 running nodes
    public final ParalelActionScenario terminate3 = new ParalelActions("terminate3") {
        public void step(int stepCount, Action... actions) {

            InvokeActions(stepCount, actions);

            if (stepCount == maxIterations/4 * 1)
                cluster.terminateRandomNode();
            if (stepCount == maxIterations/4 * 2)
                cluster.terminateRandomNode();
            if (stepCount == maxIterations/4 * 3)
                cluster.terminateRandomNode();
        }
    };


    //terminate a random node quarter's of the way through and restart it 3 quarter's of the way through
    public final ParalelActionScenario terminate1_Restart1 = new ParalelActions("terminate_DelayedRestart") {
        public void step(int stepCount, Action... actions) {

            InvokeActions(stepCount, actions);

            if (stepCount == maxIterations/4 * 1)
                cluster.terminateRandomNode();
            if (stepCount == maxIterations/4 * 3)
                cluster.addNode();
        }
    };


    //this scenario will run all instructions, and half way through terminate all cluster, wait 1 sec
    //restart cluster, and continue to run actions.
    public final ParalelActionScenario terminateAll_RestartAll = new ParalelActions("terminateAll_RestartAll"){
        public void step(int stepCount, Action... actions) {

            InvokeActions(stepCount, actions);

            if (stepCount == maxIterations/2){
                cluster.terminateAllNodes();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) { e.printStackTrace(); }
                cluster.addNode();
            }
        }
    };


    //this scenario will run all instructions, and half way through terminate all cluster, wait 1 sec
    //restart cluster, and continue to run actions.
    public final ParalelActionScenario clusterFlux = new ParalelActions("clusterFlux"){
        public void step(int stepCount, Action... actions) {

            InvokeActions(stepCount, actions);

            int everyX=250;
            if ( stepCount % everyX == everyX-1 ){

                if(cluster.isMinSize()){
                    cluster.addNode();
                }else if(cluster.isMaxSize()){
                    cluster.terminateRandomNode();
                }else if(random.nextBoolean()){
                    cluster.addNode();
                }else{
                    cluster.terminateRandomNode();
                }
            }
        }
    };
}
