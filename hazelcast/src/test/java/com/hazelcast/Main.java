package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created with IntelliJ IDEA.
 * User: alarmnummer
 * Date: 8/30/13
 * Time: 5:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    public static void main(String[]args){
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl("http://localhost:8080/mancenter-3.0.1/management-center");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
    }
}
