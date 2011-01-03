package com.channing.risk.common;

import com.channing.risk.common.Column;
import com.channing.risk.common.IRiskReportSchema;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 */
public class RiskReportSchema implements IRiskReportSchema {
    private String schemaName;
    private String createdBy;
    private Date createdOn;
    private List<Column> columns;

    public RiskReportSchema(String schemaName, String createdBy, Date createdOn, List<Column> columns){
        this.schemaName = schemaName;
        this.createdBy = createdBy;
        this.createdOn = createdOn;
        this.columns = columns;
    }

    public String getSchemaName() {
           return schemaName;
    }


    public String getCreatedBy() {
        return createdBy;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public List<Column> getColumns() {
        return columns;
    }

    /** probably should refactor this MongoDB specific code out*/
    public DBObject toDBObject(){
        DBObject result = new BasicDBObject();
        result.put("schemaName", schemaName);
        result.put("createdBy", createdBy);
        result.put("createdOn", createdOn);
        //result.put("version", version);
        List<DBObject> columnList = new LinkedList<DBObject>();
        for(Column c: columns){
            DBObject dbc = new BasicDBObject();
            dbc.put("name", c.getName());
            dbc.put("type", c.getType().toString());
            columnList.add(dbc);
        }
        result.put("columns", columnList);
        return result;
    }
}
