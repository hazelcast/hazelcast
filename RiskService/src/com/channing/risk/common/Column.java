package com.channing.risk.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 15-Jul-2010
 */
public class Column {
    private String name;
    private Class type;

    public Column(String name, Class type){
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }

    public String toString(){
        StringBuilder b = new StringBuilder();
        b.append(getName());
        b.append(",");
        b.append(getType());
        return b.toString();
    }
}
