package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapMemberListScheduledTaskHandlerCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<Member, List<ScheduledTaskHandler>>> set) {

    }

    public static List<Map.Entry<Member, List<ScheduledTaskHandler>>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}
