package com.neurospark.nerdnudge.couchbase.testers;

import com.google.gson.JsonObject;
import com.neurospark.nerdnudge.couchbase.service.NerdPersistClient;

import java.util.List;

public class NerdPersistTester {
    public static void main(String[] args) {
        NerdPersistClient nerdPersistClient = new NerdPersistClient("localhost", "Administrator",
                "sift123", "test", "_default", "_default");


        JsonObject jo = new JsonObject();
        jo.addProperty( "status", "Active" );
        jo.addProperty( "type", "plainDocument" );
        nerdPersistClient.set( "testId1", jo );
        nerdPersistClient.set( "testId2", jo );
        nerdPersistClient.set( "testId3", jo );


        nerdPersistClient.incr( "abc", 1 );
        nerdPersistClient.incr( "xyz", 100 );

        nerdPersistClient.incr( "abc", 1 );
        nerdPersistClient.incr( "xyz", 100 );
        //Thread.sleep( 10000 );

        System.out.println( "Get operation: " + nerdPersistClient.get( "testId1" ) );
        System.out.println( "Get operation for incr: " + nerdPersistClient.getCounter( "abc" ) );
        System.out.println( "Get operation for incr: " + nerdPersistClient.getCounter( "xyz" ) );

        nerdPersistClient.decr( "abc", 1 );
        nerdPersistClient.decr( "xyz", 50 );
        //Thread.sleep( 5000 );
        System.out.println( "Get operation for decr: " + nerdPersistClient.getCounter( "abc" ) );
        System.out.println( "Get operation for decr: " + nerdPersistClient.getCounter( "xyz" ) );

        nerdPersistClient.delete( "testId1" );
        nerdPersistClient.delete( "testId100" );

        System.out.println("---------------");

        String collectionName = "sde";
        List<JsonObject> objects = nerdPersistClient.getDocumentsByQuery("SELECT * FROM `content`.`quizflex`.`" + collectionName + "`", collectionName);
        for(JsonObject jsonObject: objects) {
            System.out.println(jsonObject);
        }
    }
}
