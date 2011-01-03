package com.channing.risk;

import com.mongodb.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 22-Jul-2010
 */
public class GroupTest {
    private static Mongo m;
    private static DB db;
    private static DBCollection table;

    @BeforeClass
    public static void initialiseMongo() throws Exception{
        m = new Mongo();
        db = m.getDB("GroupTest");
        table = db.getCollection("GroupTest");
    }


    @Test
    public void createTestData() throws Exception{

    }

    @Test
    public void dropTable() throws Exception{
        table.drop();
    }

    @Test
    public void testGrouping() throws Exception{
        DBObject key = new BasicDBObject();
        DBObject condition = new BasicDBObject();
        DBObject initial = new BasicDBObject();
        table.group(key,condition, initial,"");
    }


    @Test
    public void testMapReduce(){
        DBCollection c = db.getCollection( "jmr1" );
        c.drop();
        c.save( new BasicDBObject( "chars" , new String[]{ "a" , "b" } ) );
        c.save( new BasicDBObject( "chars" , new String[]{ "b" , "c" } ) );
        c.save( new BasicDBObject( "chars" , new String[]{ "c" , "d" } ) );

        DBCursor curs = c.find();
        while(curs.hasNext()){
            System.out.println(curs.next());
        }

        MapReduceOutput out =
            c.mapReduce( "function(){ for ( var i=0; i<this.chars.length; i++ ){ emit( this.chars[i] , 1 ); } }" ,
                         "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}" ,
                         null , null );

        DBCursor red = out.results();
        while(red.hasNext()){
            System.out.println(red.next());
        }

        Map<String,Integer> m = new HashMap<String,Integer>();
        for ( DBObject r : out.results() ){
            m.put( r.get( "_id" ).toString() , ((Number)(r.get( "value" ))).intValue() );
        }

        assertEquals( 4 , m.size() );
        assertEquals( 1 , m.get( "a" ).intValue() );
        assertEquals( 2 , m.get( "b" ).intValue() );
        assertEquals( 2 , m.get( "c" ).intValue() );
        assertEquals( 1 , m.get( "d" ).intValue() );
    }
    
}
