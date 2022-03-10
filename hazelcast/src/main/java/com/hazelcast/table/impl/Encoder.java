package com.hazelcast.table.impl;

import com.hazelcast.internal.nio.Packet;

public abstract class Encoder {

    public Packet packet;
    public int position;

    public abstract void encode();

    public void init(Packet packet){
        this.packet = packet;
        this.position = 0;
    }

    public void clear(){
        packet = null;
    }
}
