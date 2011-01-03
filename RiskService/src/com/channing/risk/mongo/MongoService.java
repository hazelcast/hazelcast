package com.channing.risk.mongo;

import com.channing.risk.common.*;
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 01-Aug-2010
 */
public class MongoService {
    private Mongo mongo;

    public MongoService()throws UnknownHostException{
        this.mongo = new Mongo();
    }

    public MongoService(String hostName, int port) throws UnknownHostException {
        this.mongo = new Mongo(hostName, port);
    }

    public IRiskReport getRiskReport(IReportRequest request){
        String database ="";
        String collection = "";
        Map<String, Object> query = null;
        
        DBObject riskReport = getDocument(database,collection,query);
        String schemaName = (String)riskReport.get("schemaName");
        String createdBy = (String)riskReport.get("createdBy");
        Date createdOn = (Date)riskReport.get("createdOn");
        /**@todo populate with data*/
        List<Column> columns = null;

        IRiskReportSchema schema = new RiskReportSchema(schemaName, createdBy, createdOn, columns);
        IRiskReportData data = new RiskReportData(schema);
        /**@todo populate with data*/

        return new RiskReport(schema, data);
    }
    
    /**@todo this method (eventually) should be private with Mongo independent Value Object returned */
    public DBObject getDocument(String database, String collection, Map<String,Object> query){
        DB db = mongo.getDB(database);
        DBCollection table = db.getCollection(collection);
        DBObject dbQuery = new BasicDBObject(query);
        return table.findOne(dbQuery);
    }
}
