package com.channing.risk.common;

import com.channing.risk.common.IRiskReport;
import com.channing.risk.common.IRiskReportData;
import com.channing.risk.common.IRiskReportSchema;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 */
public class RiskReport implements IRiskReport {
    // Concrete implementation of a Report
    // Defines columns with their types
    // returns a table of data

    private IRiskReportSchema schema;
    private IRiskReportData data;

    public RiskReport(IRiskReportSchema schema, IRiskReportData data){
        this.schema = schema;
        this.data = data;
    }

    public IRiskReportData getData() {
        return data;
    }

    public IRiskReportSchema getSchema() {
        return schema;
    }
}
