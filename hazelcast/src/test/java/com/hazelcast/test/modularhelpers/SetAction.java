package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.ISet;

public abstract class SetAction extends Action {
    public ISet set;

    public SetAction(String name){super(name);}

    public void after(){ System.out.println(set+" ===>>>"+ set.size()); }

    public int intResult(){return set.size();}
}
