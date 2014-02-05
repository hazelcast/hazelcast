package com.hazelcast.test.modularhelpers;


//this test passes in All the Actions for every step,  so the step at idx 0,1,2... can call all the acitons
abstract public class ParalelActionScenario extends Scenario{

    public int maxIterations=5000;

    public ParalelActionScenario(String name){super(name);}

    public void run(Action... actions){
        before();

        for(Action a: actions){
            a.before();
        }

        for(int idx=0; idx<maxIterations; idx++){
            step(idx, actions);
        }


        for(Action a: actions){
            a.after();
        }

        after();
    }

    public void step(Action... instruct){}

    public abstract void step(int idx, Action... instruct);
}






