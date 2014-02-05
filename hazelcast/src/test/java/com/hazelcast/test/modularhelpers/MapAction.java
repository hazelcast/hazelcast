package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.IMap;


public abstract class MapAction extends Action {
    public IMap map;

    public MapAction(String name){super(name);}

    public void after(){ System.out.println("after MapAction(" + this.actionName + ") " + map + " name(" + map.getName() + ") ===>>>"+ map.size()); }

    public int intResult(){return map.size();}
}