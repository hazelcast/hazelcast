package com.hazelcast.tpc;

import com.hazelcast.tpc.actor.Actor;
import com.hazelcast.tpc.actor.ActorHandle;
import com.hazelcast.tpc.actor.EchoActor;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Scheduler;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.nio.NioEventloop;

public class ActorMain {

    public static void main(String[] args){
        Eventloop eventloop = new NioEventloop();
        eventloop.setScheduler(new Scheduler() {
            @Override
            public void setEventloop(Eventloop eventloop) {
            }

            @Override
            public boolean tick() {
                return false;
            }

            @Override
            public void schedule(Frame task) {
            }
        });
        eventloop.start();

        Actor actor = new EchoActor();
        actor.activate(eventloop);

        ActorHandle handle = actor.getHandle();
        handle.send("foo");
        handle.send("bar");
    }
}
