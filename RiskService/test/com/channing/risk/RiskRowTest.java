package com.channing.risk;

import com.channing.risk.common.Column;
import com.channing.risk.common.IRiskReportSchema;
import com.channing.risk.common.RiskReportSchema;
import com.channing.risk.common.RiskRow;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 * Time: 16:02:07
 * To change this template use File | Settings | File Templates.
 */
public class RiskRowTest {
    @Test
    public void testAddValues() throws Exception {
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
        RiskRow row = new RiskRow(columns, new Object[]{new Double(1.23D),new Double(2.34D),new Double(3.45D),new Double(4.56D),
                new String("money maker")});

        List<Column> rowColumns = row.getColumns();
        assertEquals(rowColumns.get(0).getName(),"delta");
        assertEquals(rowColumns.get(0).getType(),Double.class);
        assertEquals(rowColumns.get(1).getName(),"gamma");
        assertEquals(rowColumns.get(1).getType(),Double.class);
        assertEquals(rowColumns.get(4).getName(),"trade");
        assertEquals(rowColumns.get(4).getType(),String.class);

        List rowValues = row.getValues();
        double d = (Double)rowValues.get(0);
    }
}
