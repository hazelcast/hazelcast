package com.hazelcast.tpc.engine.actor;

public interface ActorHandle {

    void send(Object message);

}
