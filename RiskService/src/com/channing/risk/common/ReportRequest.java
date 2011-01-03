package com.channing.risk.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 29-Jul-2010
 */

public class ReportRequest {
    private Map<String, Object> requests = new HashMap<String,Object>();

    public ReportRequest(){}

    public void put(String key, Object value){
        requests.put(key,value);
    }

    public Set<Map.Entry<String,Object>> getRequests(){
        return requests.entrySet();
    }

    public Set<String> getKeys(){
        return requests.keySet();
    }

    public Map<String,Object> getMap(){
        return requests;
    }
}
