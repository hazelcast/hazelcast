package com.hazelcast.tpc.actor;

public class ActorHandle {

    private final Actor actor;

    public ActorHandle(Actor actor) {
        this.actor = actor;
    }

    public void send(Object message){
        this.actor.send(message);
    }
}
