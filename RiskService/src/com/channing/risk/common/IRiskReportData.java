package com.channing.risk.common;

import com.mongodb.DBObject;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public interface IRiskReportData {
    void addRow(RiskRow row) throws InvalidDataException;
    List<RiskRow> getRows();
}
