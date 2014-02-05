package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.ReplicatedMap;

public abstract class RepMapAction extends Action {
    public ReplicatedMap map;

    public RepMapAction(String name){super(name);}

    public void after(){ System.out.println(map+" ===>>>"+ map.size()); }

    public int intResult(){return map.size();}
}