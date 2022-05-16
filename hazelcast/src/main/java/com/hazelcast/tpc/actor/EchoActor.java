package com.hazelcast.tpc.actor;

public class EchoActor extends Actor{

    public EchoActor() {
    }

    @Override
    public void process(Object msg) {
        System.out.println(msg);
    }
}
