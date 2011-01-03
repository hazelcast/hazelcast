package com.channing.risk.common;

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
public class RiskReportData implements IRiskReportData {
    private IRiskReportSchema schema;
    private List<RiskRow> data = new LinkedList<RiskRow>();

    public RiskReportData(IRiskReportSchema schema, List<RiskRow> rows)
            throws InvalidDataException{
        this.schema = schema;
    }

    public RiskReportData(IRiskReportSchema schema) {
        this.schema = schema;
    }

    public void addRow(RiskRow row) throws InvalidDataException{
        data.add(row);
    }

    private void checkDataAgainstSchema(RiskRow row) throws InvalidDataException{
        List<Column> rowCols = row.getColumns();
        if(!schema.getColumns().equals(rowCols)){
            throw new InvalidDataException("Data does not match Schema");
        };
    }

     public IRiskReportSchema getSchema() {
        return schema;
    }

    public List<RiskRow> getRows() {
        return data;
    }
}
