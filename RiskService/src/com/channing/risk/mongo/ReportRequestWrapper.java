package com.channing.risk.mongo;

import com.channing.risk.common.IReportRequest;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 29-Jul-2010
 */
public class ReportRequestWrapper {
    private IReportRequest request;
    public ReportRequestWrapper(IReportRequest request){
        this.request = request;
    }

    public DBObject toDBObject(){
        return new BasicDBObject(request.getMap());
    }
}
