package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.core.HazelcastJsonValue;

import java.io.Serializable;

public class ComplexObject implements Serializable {
    private Long id;
    private HazelcastJsonValue jsonValue;

    public ComplexObject() {
    }

    public ComplexObject(final Long id, final HazelcastJsonValue jsonValue) {
        this.id = id;
        this.jsonValue = jsonValue;
    }

    public Long getId() {
        return id;
    }

    public void setId(final Long id) {
        this.id = id;
    }

    public HazelcastJsonValue getJsonValue() {
        return jsonValue;
    }

    public void setJsonValue(final HazelcastJsonValue jsonValue) {
        this.jsonValue = jsonValue;
    }
}
