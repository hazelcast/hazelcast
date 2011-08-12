package com.hazelcast.hibernate.entity;

import java.util.Date;

public class DummyEntity {
	
	private long id;
	
	private int version;
	
	private String name;
	
	private double value;
	
	private Date date;
	
	public DummyEntity() {
		super();
	}

	public DummyEntity(long id, String name, double value, Date date) {
		super();
		this.id = id;
		this.name = name;
		this.value = value;
		this.date = date;
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
}
