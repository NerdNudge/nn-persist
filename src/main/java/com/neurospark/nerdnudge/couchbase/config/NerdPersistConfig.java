package com.neurospark.nerdnudge.couchbase.config;

import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;

public class NerdPersistConfig {
    public static Cluster syncCluster;
    private static AsyncCluster asyncCluster;

    private static String persistAddress;
    private static String persistUsername;
    private static String persistPassword;

    private static String bucketName;
    private static String scopeName;
    private static String collectionName;

    private static Bucket syncBucket;
    private static AsyncBucket asyncBucket;

    private static AsyncCollection asyncCollection;

    private static Collection syncCollection;
    private static ClusterEnvironment env;

     public static void init(final String persistAddress, final String persistUsername, final String persistPassword, final String bucketName, final String scopeName, final String collectionName) {
        env = ClusterEnvironment.builder().build();
        System.out.println( "Persist Address: " + persistAddress );

        NerdPersistConfig.persistAddress = persistAddress;
        NerdPersistConfig.persistUsername = persistUsername;
        NerdPersistConfig.persistPassword = persistPassword;

        NerdPersistConfig.bucketName = bucketName;
        NerdPersistConfig.scopeName = scopeName;
        NerdPersistConfig.collectionName = collectionName;

        initializeSyncClusters();
        initializeAsyncClusters();
    }

    private static void initializeSyncClusters() {
        initializeClusters(true);
    }

    private static void initializeAsyncClusters() {
        initializeClusters(false);
    }

    private static void initializeClusters(final boolean sync) {
        if(sync) {
            if(syncCluster == null) {
                initializeSyncCluster();
                initializeSyncBucket();
                initializeSyncCollection();
            }
        }
        else {
            if(asyncCluster == null) {
                initializeAsyncCluster();
                initializeAsyncBucket();
                initializeAsyncCollection();
            }
        }
    }

    private static void initializeSyncCluster() {
        syncCluster = Cluster.connect(persistAddress, persistUsername, persistPassword);
    }

    private static void initializeSyncBucket() {
        syncBucket = syncCluster.bucket(bucketName);
        syncBucket.waitUntilReady(Duration.ofSeconds(120));
    }

    private static void initializeSyncCollection() {
        syncCollection = syncBucket.scope(scopeName).collection(collectionName);
        System.out.println( "************************************* Initialised Sync CouchbaseClient ********** " + syncCollection.toString() );
    }

    private static void initializeAsyncCluster() {
        asyncCluster = Cluster.connect(persistAddress, ClusterOptions.clusterOptions(persistUsername, persistPassword).environment(env)).async();
    }

    private static void initializeAsyncBucket() {
        asyncBucket = asyncCluster.bucket(bucketName);
        asyncBucket.waitUntilReady(Duration.ofSeconds(120));
    }

    private static void initializeAsyncCollection() {
        asyncCollection = asyncBucket.scope(scopeName).collection(collectionName);
        System.out.println( "************************************* Initialised Async CouchbaseClient ********** " + asyncCollection.toString() );
    }

    public static AsyncCollection getAsyncCollection() {
        return asyncCollection;
    }

    public static Collection getSyncCollection() {
        return syncCollection;
    }
}
