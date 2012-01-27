package com.hazelcast.hibernate.entity;

public class DummyProperty {

    private long id;

    private int version;

    private String key;

    private DummyEntity entity;

    public DummyProperty() {
    }

    public DummyProperty(String key) {
        super();
        this.key = key;
    }

    public DummyProperty(String key, DummyEntity entity) {
        super();
        this.key = key;
        this.entity = entity;
    }

    public DummyProperty(long id, String key, DummyEntity entity) {
        super();
        this.id = id;
        this.key = key;
        this.entity = entity;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public DummyEntity getEntity() {
        return entity;
    }

    public void setEntity(DummyEntity entity) {
        this.entity = entity;
    }
}
