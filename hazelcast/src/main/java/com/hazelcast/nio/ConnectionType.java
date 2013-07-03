package com.hazelcast.nio;

/**
* @author mdogan 7/3/13
*/
public enum ConnectionType {

    NONE(false, false),
    MEMBER(true, true),
    JAVA_CLIENT(false, true),
    CSHARP_CLIENT(false, true),
    CPP_CLIENT(false, true),
    PYTHON_CLIENT(false, true),
    RUBY_CLIENT(false, true),
    BINARY_CLIENT(false, true),
    REST_CLIENT(false, false),
    MEMCACHE_CLIENT(false, false);

    final boolean member;
    final boolean binary;

    ConnectionType(boolean member, boolean binary) {
        this.member = member;
        this.binary = binary;
    }

    public boolean isBinary() {
        return binary;
    }

    public boolean isClient() {
        return !member;
    }
}
