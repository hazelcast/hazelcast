package com.hazelcast.test.modularhelpers;

//this Scenario runs each action in turn, every itteration the current action is called then the step.
//actions run in serial,  acton then step is called, maxIterations.
abstract public class SerialActionScenario extends Scenario{

    protected int maxIterations=5000;

    public SerialActionScenario(String name){super(name);}

    public void run(Action... actions){
        before();

        for(Action a : actions){
            a.before();
            for(int idx=0; idx<maxIterations; idx++){
                a.call(idx);
                step(idx);
            }
            a.after();
        }

        after();
    }


    public void step(Action... actions){}

    public abstract void step(int idx);
}