package com.channing.risk.common;

import com.mongodb.DBObject;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public interface IRiskReportSchema {
    String getSchemaName();
    String getCreatedBy();
    Date getCreatedOn();
    //int getVersion();
    List<Column> getColumns();
    DBObject toDBObject();
}
