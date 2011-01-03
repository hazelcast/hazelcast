package com.channing.risk.mongo;

import com.channing.risk.common.*;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public class RiskReportWrapperTest{

    @Test
    public void testToDBObjectList() throws Exception {
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
        IRiskReport report = new RiskReport(schema,data);
        for(int i=0;i<5;++i){
            RiskRow r = new RiskRow(columns, new Object[]{new Double(i/1.2),new Double(i*i/2.34),
                    new Double(i*i*i/4.56), new Double(i*i*i/46) ,new String("trade" + i)});
            data.addRow(r);
        }
        RiskReportWrapper wrapper = new RiskReportWrapper(report);
        List<DBObject> rows = wrapper.toDBObjectList();
    }
}
