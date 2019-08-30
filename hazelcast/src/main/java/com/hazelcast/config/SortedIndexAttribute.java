package com.hazelcast.config;

public class SortedIndexAttribute {

    public static final boolean DEFAULT_ASC = true;

    private String name;
    private boolean asc = DEFAULT_ASC;

    public SortedIndexAttribute(String name) {
        this.name = name;
    }

    public SortedIndexAttribute(String name, boolean asc) {
        this.name = name;
        this.asc = asc;
    }

    public String getName() {
        return name;
    }

    public SortedIndexAttribute setName(String name) {
        this.name = name;

        return this;
    }

    public boolean isAsc() {
        return asc;
    }

    public SortedIndexAttribute setAsc(boolean asc) {
        this.asc = asc;

        return this;
    }



    @Override
    public String toString() {
        return "SortedIndexAttribute{name=" + name + ", asc=" + asc + '}';
    }
}
