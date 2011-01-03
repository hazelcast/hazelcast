package com.channing.risk.common;

import com.channing.risk.common.Column;
import com.channing.risk.common.InvalidDataException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public class RiskRow implements Serializable {
    private List values = new ArrayList();
    private List<Column> columns;

    public RiskRow(List<Column> columns, Object[] values)throws InvalidDataException{
        this.columns = columns;
        if(values.length!=columns.size()){
            throw new InvalidDataException( "The data has the wrong number of columns");
        }
        int i=0;
        for(Column c: columns){
            checkType(values[i],c.getType());
            this.values.add(values[i]);
            ++i;
        }
    }

    private void checkType(Object value, Class expected) throws InvalidDataException{
        if(!(value.getClass().equals(expected))){
            throw new InvalidDataException();
        }
    }

    public List getValues() {
        return values;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
