package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.Message;
import com.hazelcast.topic.ReliableMessageListener;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.synchronizedList;

public class ReliableMessageListenerMock implements ReliableMessageListener<String> {

    public final List<String> objects = synchronizedList(new ArrayList<String>());
    public final List<Message<String>> messages = synchronizedList(new ArrayList<Message<String>>());
    public volatile long storedSequence;
    public volatile boolean isLossTolerant = false;
    public volatile long initialSequence = -1;
    public volatile boolean isTerminal = true;

    @Override
    public void onMessage(Message<String> message) {
        objects.add(message.getMessageObject());
        messages.add(message);
        System.out.println(message.getMessageObject());
    }

    @Override
    public long retrieveInitialSequence() {
        return initialSequence;
    }

    @Override
    public void storeSequence(long sequence) {
        storedSequence = sequence;
    }

    @Override
    public boolean isLossTolerant() {
        return isLossTolerant;
    }

    @Override
    public boolean isTerminal(Throwable failure) {
        return isTerminal;
    }

    public void clean() {
        objects.clear();
        messages.clear();
    }
}
