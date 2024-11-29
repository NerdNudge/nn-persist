package com.neurospark.nerdnudge.couchbase.config;

import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class NerdPersistConfig {
    public Cluster syncCluster;
    private AsyncCluster asyncCluster;

    private String persistAddress;
    private String persistUsername;
    private String persistPassword;

    private String bucketName;
    private String scopeName;
    private String collectionName;

    private Bucket syncBucket;
    private AsyncBucket asyncBucket;

    private AsyncCollection asyncCollection;

    private Collection syncCollection;
    private ClusterEnvironment env;

    public void init(final String persistAddress, final String persistUsername, final String persistPassword, final String bucketName, final String scopeName, final String collectionName) {
        env = ClusterEnvironment.builder().build();
        log.info("Persist Address: {}", persistAddress);

        this.persistAddress = persistAddress;
        this.persistUsername = persistUsername;
        this.persistPassword = persistPassword;

        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionName = collectionName;

        initializeSyncClusters();
        initializeAsyncClusters();
    }

    private void initializeSyncClusters() {
        initializeClusters(true);
    }

    private void initializeAsyncClusters() {
        initializeClusters(false);
    }

    private void initializeClusters(final boolean sync) {
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

    private void initializeSyncCluster() {
        syncCluster = Cluster.connect(persistAddress, persistUsername, persistPassword);
    }

    private void initializeSyncBucket() {
        syncBucket = syncCluster.bucket(bucketName);
        syncBucket.waitUntilReady(Duration.ofSeconds(120));
    }

    private void initializeSyncCollection() {
        syncCollection = syncBucket.scope(scopeName).collection(collectionName);
        log.info( "************************************* Initialised Sync CouchbaseClient ********** {}", syncCollection.toString() );
    }

    private void initializeAsyncCluster() {
        asyncCluster = Cluster.connect(persistAddress, ClusterOptions.clusterOptions(persistUsername, persistPassword).environment(env)).async();
    }

    private void initializeAsyncBucket() {
        asyncBucket = asyncCluster.bucket(bucketName);
        asyncBucket.waitUntilReady(Duration.ofSeconds(120));
    }

    private void initializeAsyncCollection() {
        asyncCollection = asyncBucket.scope(scopeName).collection(collectionName);
        log.info( "************************************* Initialised Async CouchbaseClient ********** {}", asyncCollection.toString() );
    }

    public AsyncCollection getAsyncCollection() {
        return asyncCollection;
    }

    public Collection getSyncCollection() {
        return syncCollection;
    }
}
