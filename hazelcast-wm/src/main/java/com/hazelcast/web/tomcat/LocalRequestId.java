package com.hazelcast.web.tomcat;

public class LocalRequestId {

    private static final ThreadLocal<Long> requestId = new ThreadLocal<Long>();

    public static void set(long id) {
        requestId.set(id);
    }

    public static Long get() {
        return requestId.get();
    }

    public static void reset() {
        requestId.remove();
    }
}
