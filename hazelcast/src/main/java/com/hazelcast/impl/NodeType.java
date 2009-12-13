/**
 * 
 */
package com.hazelcast.impl;

public enum NodeType {
    MEMBER(1),
    SUPER_CLIENT(2),
    JAVA_CLIENT(3),
    CSHARP_CLIENT(4);

    private int value;

    private NodeType(int type) {
        this.value = type;
    }

    public int getValue() {
        return value;
    }

    public static NodeType create(int value) {
        switch (value) {
            case 1:
                return MEMBER;
            case 2:
                return SUPER_CLIENT;
            case 3:
                return JAVA_CLIENT;
            case 4:
                return CSHARP_CLIENT;
            default:
                return null;
        }
    }
}