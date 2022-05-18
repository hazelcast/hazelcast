package com.hazelcast.tpc;

import com.hazelcast.tpc.engine.actor.Actor;
import com.hazelcast.tpc.engine.actor.EchoActor;
import com.hazelcast.tpc.engine.actor.LocalActorHandle;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop;

public class ActorMain {

    public static void main(String[] args){
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        Actor actor = new EchoActor();
        actor.activate(eventloop);

        LocalActorHandle handle = actor.getHandle();
        handle.send("foo");
        handle.send("bar");
    }

}
