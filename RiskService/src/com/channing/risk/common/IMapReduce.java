package com.channing.risk.common;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 12-Aug-2010
 * Generates JavaScript for Map-Reduce operations in MongoDB
 */
public interface IMapReduce {
    String getMapScript();
    String getReduceScript();
}
