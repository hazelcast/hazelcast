package com.channing.risk;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 18-Jul-2010
 * Time: 23:38:38
 * To change this template use File | Settings | File Templates.
 */
public class DBTest {
    private static Mongo m;

    @BeforeClass
    public static void initialiseMongo() throws Exception{
        m = new Mongo();
    }

    @Test
    public void testGetDB() throws Exception{
        DB local = m.getDB("local");
        Set<String> tables = local.getCollectionNames();
        for(String t: tables){
            System.out.println(t);
        }
        DB admin = m.getDB("admin");
        Set<String> adminTables = admin.getCollectionNames();
        for(String t: adminTables){
            System.out.println(t);
        }
    }
}
