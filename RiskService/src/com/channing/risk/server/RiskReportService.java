package com.channing.risk.server;

import com.channing.risk.common.*;
import com.mongodb.*;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 * High level service interface, that could be exposed as a web service.  It completely hides the underlying MongoDB
 * implementation exposing only basic value objects
 */

public class RiskReportService {

    private Mongo server;
    private DB database;
    private DBCollection riskData;
    private DBCollection riskReports;
    private DBCollection riskSchema;


    public RiskReportService(Mongo server){
        this.server = server;
        /** should move this into Spring config?*/
        this.database = server.getDB("risk");
        this.riskData = database.getCollection("riskData");
        this.riskReports = database.getCollection("riskReports");
        this.riskSchema = database.getCollection("riskSchema");
    }

    /** Used to create or update risk report schemas*/
    public void saveSchema(IRiskReportSchema schema){
        DBObject query = new BasicDBObject();
        query.put("schemaName",schema.getSchemaName());
        DBObject dbSchema = schema.toDBObject();
        riskSchema.save(dbSchema);

    }

    public IRiskReportSchema getSchema(String name){
        DBObject query = new BasicDBObject();
        query.put("schemaName", name);
        DBObject dbSchema = riskSchema.findOne(query);
        return null;
    }

    public List<String> getSchemaNames(){
       return riskSchema.distinct("schemaName");
    }

    public long getSchemaCount(){
        return riskSchema.count();
    }

    /**Used to persist risk reports that conform to the specified schema which will
     * also be created if it does not already exist */
    public void saveRiskReport(IRiskReportSchema schema, IRiskReportData data){
        // Validate the report against the schema, skipped for now
        
    }

    public IRiskReport getRiskReport(IReportRequest request){
        DBObject query = new BasicDBObject(request.getMap());

        DBObject dbSchema = riskSchema.findOne(query);

        List<DBObject> dbColumns = (List<DBObject>)dbSchema.get("columns");
        List<Column> columns = null;

        String schemaName = (String)dbSchema.get("schemaName");
        String createdBy = (String)dbSchema.get("createdBy");
        Date createdOn = (Date)dbSchema.get("createdOn");
        IRiskReportSchema schema = new RiskReportSchema(schemaName,createdBy,createdOn,columns);

        DBCursor cursor = riskData.find();
        while(cursor.hasNext()){
            DBObject row = cursor.next();
        }
        return null;
    }

    public IRiskReport getRiskReport(IReportRequest request, IMapReduce aggregator){
        return null;
    }

    public IRiskReport getRiskReport(IRiskReport request, String groupByColumn){
        DBObject key = new BasicDBObject();
        DBObject condition = new BasicDBObject();
        DBObject initial = new BasicDBObject();
        String reduce = "";
        DBObject result = riskData.group(key,condition, initial, reduce);

        return null;
    }
}
