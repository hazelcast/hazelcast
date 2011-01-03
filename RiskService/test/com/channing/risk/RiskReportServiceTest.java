package com.channing.risk;

import com.channing.risk.common.*;
import com.channing.risk.server.RiskReportService;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.junit.Test;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 */
public class RiskReportServiceTest {
    
    @Test
    public void testGetRiskReport() throws Exception {
    }

    public void testGetAvailableReports() throws Exception {
    }

    public void testCreateReport() throws Exception {
    }

    @Test
    public void testSaveSchema() throws Exception {
        RiskReportService service = new RiskReportService(new Mongo());
        List<Column> columns = new LinkedList<Column>();
        Column delta = new Column("delta", Double.class);
        columns.add(delta);
        Column gamma = new Column("gamma", Double.class);
        columns.add(gamma);
        Column vega = new Column("vega", Double.class);
        columns.add(vega);
        Column theta = new Column("theta", Double.class);
        columns.add(theta);
        Column trade = new Column("trade", String.class);
        columns.add(trade);
        Column book = new Column("book", String.class);
        columns.add(book);
        IRiskReportSchema schema = new RiskReportSchema("dailyRisk", "jchanning", new Date(), columns);
        service.saveSchema(schema);
    }

    @Test
    public void testFindSchema() throws Exception {
        RiskReportService service = new RiskReportService(new Mongo());
        DBObject query = new BasicDBObject();
    }

     @Test
    public void testSaveRiskReport() throws Exception {
     List<Column> columns = new LinkedList<Column>();
        Column delta = new Column("delta", Double.class);
        columns.add(delta);
        Column gamma = new Column("gamma", Double.class);
        columns.add(gamma);
        Column vega = new Column("vega", Double.class);
        columns.add(vega);
        Column theta = new Column("theta", Double.class);
        columns.add(theta);
        Column trade = new Column("trade", String.class);
        columns.add(trade);

        IRiskReportSchema schema = new RiskReportSchema("test","jchanning", new Date(),columns);
        IRiskReportData data = new RiskReportData(schema);

        for(int i=0;i<100;++i){
            RiskRow r = new RiskRow(columns, new Object[]{new Double(i/1.2),new Double(i*i/2.34),
                    new Double(i*i*i/4.56), new Double(i*i*i/46) ,new String("trade" + i)});
            data.addRow(r);
        }

         RiskReportService service = new RiskReportService(new Mongo());
         service.saveRiskReport(schema, data);
     }

    @Test
    public void testGetSchemas() throws Exception{
        RiskReportService service = new RiskReportService(new Mongo());
        List<String> schemas = service.getSchemaNames();
        for(String sch: schemas){
            System.out.println(sch);
        }
    }

}
