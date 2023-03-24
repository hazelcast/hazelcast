package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.type.QueryDataType;
import org.bson.Document;

import java.io.Serializable;

class ProjectionData implements Serializable {

    final String externalName;
    final Document projectionExpr;
    final int index;
    final QueryDataType type;

    ProjectionData(String externalName, Document projectionExpr, int index, QueryDataType type) {
        this.externalName = externalName;
        this.projectionExpr = projectionExpr;
        this.index = index;
        this.type = type;
    }
}
