package com.channing.risk.mongo;

import com.channing.risk.common.Column;
import com.channing.risk.common.IRiskReport;
import com.channing.risk.common.RiskRow;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public class RiskReportWrapper {
    private IRiskReport report;

    public RiskReportWrapper(IRiskReport report){
        this.report = report;
    }

    public List<DBObject> toDBObjectList(){
        //created on the fly, the data should be cached if used repeatedly
        List<DBObject> result = new LinkedList<DBObject>();
        for(RiskRow r: report.getData().getRows()){
            DBObject dbRow = new BasicDBObject();
            List vals = r.getValues();
            Iterator iter = vals.iterator();
            for(Column c: report.getSchema().getColumns()){
                String name = c.getName();
                Object value = iter.next();
                dbRow.put(name, value);
            }
            result.add(dbRow);
        }
        return result;
    }
}
