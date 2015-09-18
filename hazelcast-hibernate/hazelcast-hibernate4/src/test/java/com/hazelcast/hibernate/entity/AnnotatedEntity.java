package com.hazelcast.hibernate.entity;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NaturalIdCache;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@NaturalIdCache
@Entity
@Table( name = "ANNOTATED_ENTITIES" )
public class AnnotatedEntity {
    private Long id;

    private String title;

    public AnnotatedEntity() {
    }

    public AnnotatedEntity(String title) {
        this.title = title;
    }

    @Id
    @GeneratedValue(generator="increment")
    @GenericGenerator(name="increment", strategy = "increment")
    public Long getId() {
        return id;
    }

    private void setId(Long id) {
        this.id = id;
    }

    @NaturalId(mutable = true)
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}