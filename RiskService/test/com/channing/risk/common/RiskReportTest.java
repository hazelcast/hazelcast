package com.channing.risk.common;

import org.junit.Test;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 29-Jul-2010
 */
public class RiskReportTest {

    @Test
    public void testRiskReportCreation() throws Exception{
        List<Column> columns = new LinkedList<Column>();
            columns.add(new Column("delta",Double.class));
            columns.add(new Column("gamma",Double.class));
        IRiskReportSchema schema = new RiskReportSchema("test","jchanning", new Date(),columns);

        IRiskReportData data = new RiskReportData(schema);
        RiskRow r = new RiskRow(columns,new Object[]{new Double(1.234), new Double(2.34)});
        data.addRow(r);
            r = new RiskRow(columns, new Object[]{new Double(6.666), new Double(180)});
        data.addRow(r);


        IRiskReport report = new RiskReport(schema, data);

        IRiskReportData d = report.getData();
        IRiskReportSchema s = report.getSchema();
        assertEquals(data, d);
        assertEquals(schema, s);

        List<RiskRow> l = d.getRows();
        for(RiskRow rr : l){
            for(Object o:rr.getValues()){
                System.out.print(o + ",");
            }
            System.out.println();
        }
    }
}
