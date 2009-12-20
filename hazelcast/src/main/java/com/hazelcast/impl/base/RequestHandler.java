/**
 * 
 */
package com.hazelcast.impl.base;

import com.hazelcast.impl.Request;

public interface RequestHandler {
    void handle(Request request);
}