package com.hazelcast.wm.test;

public interface ServletContainer {

    public void restart() throws Exception;

    public void stop() throws Exception;

    public void start() throws Exception;
}
