package com.channing.risk.common;

import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 14-Jul-2010
 */
public interface IReportRequest {
    Set<String> getKeys();
    Map<String,Object> getMap();
}
