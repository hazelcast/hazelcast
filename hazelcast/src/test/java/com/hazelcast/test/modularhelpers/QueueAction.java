package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.IQueue;

public abstract class QueueAction extends Action {
    public IQueue queue;

    public QueueAction(String name){super(name);}

    public void after(){ System.out.println(queue + " ===>>>" + queue.size()); }

    public int intResult(){return queue.size();}
}