package com.hazelcast.test.modularhelpers;

abstract public class Scenario {

    public String scenarioName;

    public Scenario(String name){this.scenarioName =name;}

    public abstract void before();

    public void run(Action... actions){
        before();

        for(Action i: actions)
            i.before();

        step(actions);    //what a bout an after step call back objects, diffrent one for each subclass,  as this will just pass Actions,  the Serial will pass just idx
                          //and the iterated idx and actions.      Then I can do asserts after every step,  eg
        for(Action i: actions)
            i.after();

        after();
    }

    public abstract void after();

    public abstract void step(Action... actions);
}
