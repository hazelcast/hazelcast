package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.spi.impl.engine.frame.Frame;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Requests for a given member.
 */
public class Requests {
    final ConcurrentMap<Long, Frame> map = new ConcurrentHashMap<>();
    final AtomicLong callId = new AtomicLong(500);

    public void handleResponse(Frame response){

    }
}
