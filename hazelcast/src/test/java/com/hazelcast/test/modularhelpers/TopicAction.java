package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;


public abstract class TopicAction extends Action {
    public ITopic topic;
    public int count=0;


    public TopicAction(String name){super(name);}

    public void before(){topic.addMessageListener(new MessageListener() {
        public void onMessage(Message message) {
            count++;
        }
    });}


    public void after(){ System.out.println(topic + " ===>>>" + count); }

    public int intResult(){return count;}
}