package com.channing.risk;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 20-Jul-2010
 */
public class CollectionTest {
    private static Mongo m;
    private static DB db;

    @BeforeClass
    public static void initialiseMongo() throws Exception{
        m = new Mongo();
        db = m.getDB("local");
    }

    @Test
    public void test() throws Exception{
        DBCollection coll = db.getCollection("test");
    }
}
