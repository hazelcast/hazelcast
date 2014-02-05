package com.hazelcast.test.modularhelpers;

public abstract class Action {

    public String actionName;
    Action(String name){this.actionName=name;}

    public void before(){}
    public abstract void call(final int i);
    public void after(){}

    public int intResult(){throw new Error("not implemented");}
}
