package com.hazelcast.spring.transaction;

import java.io.Serializable;

public class DummyObject implements Serializable {

    private Long id;
    private String string;


    public DummyObject() {
    }

    public DummyObject(long id, String string) {
        this.id = id;
        this.string = string;
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }
}
