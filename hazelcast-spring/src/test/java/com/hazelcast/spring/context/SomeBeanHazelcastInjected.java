package com.hazelcast.spring.context;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class SomeBeanHazelcastInjected {

    @Autowired
    @Qualifier("instance1")
    private HazelcastInstance instance;

    public HazelcastInstance getInstance() {
        return instance;
    }
}