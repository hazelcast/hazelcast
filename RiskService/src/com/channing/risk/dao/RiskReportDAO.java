package com.channing.risk.dao;

import com.channing.risk.common.*;
import com.mongodb.DBObject;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 * Allows an IRiskReport object to be built from it MongoDB DBObject representation
 */
public class RiskReportDAO {
    private DBObject riskReport;

    public RiskReportDAO(DBObject riskReport){
        this.riskReport = riskReport;
    }

    public IRiskReport getRiskReport(){
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
}
