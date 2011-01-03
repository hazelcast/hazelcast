package com.channing.risk;

import com.mongodb.*;
import org.junit.Test;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 */
public class CreateTestData {

    @Test
    public void createTestRiskData() throws Exception{
        Mongo server = new Mongo();
        DB database = server.getDB("riskDB");
        DBCollection table = database.getCollection("riskData");
        for(int i=0; i<100; ++i){
            DBObject row = new BasicDBObject();
                row.put("tradeId", i);
                row.put("businessDate", new Date());
                row.put("delta", 0.4*i*i);
                row.put("putCall", "put");
                row.put("expiry", new Date());
            table.insert(row);
        }
        server.close();
    }

    @Test
    public void readTestData() throws Exception{
        Mongo server = new Mongo();
        DB database = server.getDB("riskDB");
        DBCollection table = database.getCollection("riskData");
        DBCursor cursor = table.find();
        while(cursor.hasNext()){
            DBObject o = cursor.next();
            int id = (Integer)o.get("tradeId");
            Date businessDate = (Date)o.get("businessDate");
            System.out.println(id);
            System.out.println(businessDate);
        }
    }
}
