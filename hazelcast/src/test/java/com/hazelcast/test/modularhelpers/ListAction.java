package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.IList;


public abstract class ListAction extends Action {
    public IList list;

    public ListAction(String name){super(name);}

    public void after(){ System.out.println(list+" ===>>>"+ list.size()); }

    public int intResult(){return list.size();}
}