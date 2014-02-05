package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.ILock;

public abstract class LockAction extends Action {
    public ILock lock;

    public LockAction(String name){super(name);}

    public void after(){ System.out.println(lock+" ===>>>"+ lock.getLockCount()); }

    public int intResult(){return lock.getLockCount();}
}