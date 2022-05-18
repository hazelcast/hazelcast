package com.hazelcast.tpc.engine.actor;

public class EchoActor extends Actor{

    public EchoActor() {
    }

    @Override
    public void process(Object msg) {
        System.out.println(msg);
    }
}
