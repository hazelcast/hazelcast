package com.channing.risk;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 18-Jul-2010
 */
public class MongoTest {
    private static Mongo m;

    @BeforeClass
    public static void initialiseMongo() throws Exception{
        m = new Mongo();
    }

    @Test
    public void testGetAddress() throws Exception{
        ServerAddress address = m.getAddress();
        System.out.println(address.getHost() + ":" + address.getPort());
        System.out.println(address.getSocketAddress());
    }

    @Test
    public void testDebugString() throws Exception{
        System.out.println(m.debugString());
    }

    @Test
    public void testConnectPoint() throws Exception{
        System.out.println(m.getConnectPoint());
    }

    @Test
    public void testDatabaseName() throws Exception{
        List<String> names = m.getDatabaseNames();
        for(String db: names){
            System.out.println(db);
        }
    }

    @Test
    public void testVersion() throws Exception{
        System.out.println(m.getVersion());
    }
}
