package com.hazelcast.tpc.actor;

public interface ActorHandle {

    void send(Object message);
}
