package com.hazelcast.pipeline.impl;

public class FlushThreadLocal {

    private final static ThreadLocal<Boolean> FLUSH = new ThreadLocal<Boolean>(){
        @Override
        protected Boolean initialValue() {
            return true;
        }
    };

    public static boolean flush(){
        return FLUSH.get();
    }

    public static void flush(boolean flush){
        FLUSH.set(flush);
    }
}
