package com.channing.risk;

import com.channing.risk.common.*;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.List;

import java.util.Date;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public class RiskReportDataTest {
    private IRiskReportData data;
    private IRiskReportSchema schema;

    @Before
    public void testAddRow() throws Exception {
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

        IRiskReportSchema schema = new RiskReportSchema("test", "jchanning",new Date(),
                columns);
        this.schema = new RiskReportSchema("test","jchanning", new Date(),columns);
        this.data = new RiskReportData(schema);

        for(int i=0;i<5;++i){
            RiskRow r = new RiskRow(columns, new Object[]{new Double(i/1.2),new Double(i*i/2.34),
                    new Double(i*i*i/4.56), new Double(i*i*i/46) ,new String("trade" + i)});
            data.addRow(r);
        }

    }

    @Test
    public void getSchema() throws Exception{
        assertEquals("test", schema.getSchemaName());
        assertEquals("jchanning", schema.getCreatedBy());
        for(Column c: schema.getColumns()){
            System.out.println(c);
        }
    }
}
