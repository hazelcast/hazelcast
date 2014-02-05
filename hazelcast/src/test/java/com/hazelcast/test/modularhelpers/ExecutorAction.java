package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.IExecutorService;

public abstract class ExecutorAction extends Action {
    public IExecutorService ex;
    public int totalCalls=0;

    public ExecutorAction(String name){super(name);}

    public void after(){ System.out.println("after ExecutorAction(" + this.actionName + ") " + ex + " name(" + ex.getName() + ") ===>>>"+ totalCalls); }

    public int intResult(){return totalCalls;}
}