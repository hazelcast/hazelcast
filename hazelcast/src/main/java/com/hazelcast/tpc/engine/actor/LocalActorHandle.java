package com.hazelcast.tpc.engine.actor;

public class LocalActorHandle implements ActorHandle {

    private final Actor actor;

    public LocalActorHandle(Actor actor) {
        this.actor = actor;
    }

    @Override
    public void send(Object message){
        this.actor.send(message);
    }
}
