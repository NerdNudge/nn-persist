package com.neurospark.nerdnudge.couchbase.service;

import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neurospark.nerdnudge.couchbase.config.NerdPersistConfig;
import com.neurospark.nerdnudge.couchbase.utils.GsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class NerdPersistClient implements NerdPersist {

    private AsyncCollection asyncCollection;
    private Collection syncCollection;
    private JsonTranscoder transcoder;
    private JsonParser jsonParser;
    private NerdPersistConfig nerdPersistConfig;

    public NerdPersistClient(final String persistAddress, final String persistUsername, final String persistPassword, final String bucketName, final String scopeName, final String collectionName) {
        nerdPersistConfig = new NerdPersistConfig();
        nerdPersistConfig.init(persistAddress, persistUsername, persistPassword, bucketName, scopeName, collectionName);
        syncCollection = nerdPersistConfig.getSyncCollection();
        asyncCollection = nerdPersistConfig.getAsyncCollection();

        GsonSerializer serializer = new GsonSerializer();
        transcoder = JsonTranscoder.create(serializer);
        jsonParser = new JsonParser();
    }

    public void set(String id, int expiry, JsonObject data) {
        try {
            asyncCollection.upsert(id, data, UpsertOptions.upsertOptions().expiry(Duration.ofSeconds(expiry)).transcoder(transcoder));
        }
        catch (Exception e) {
            System.err.println("Error setting document: " + e.getMessage());
        }
    }

    public void set(String id, JsonObject data) {
        try {
            asyncCollection.upsert(id, data, UpsertOptions.upsertOptions().transcoder(transcoder));
        }
        catch (Exception e) {
            System.err.println("Error setting document: " + e.getMessage());
        }
    }

    public void delete(String id) {
        asyncCollection.remove(id);
    }

    public JsonObject get(String id) {
        try {
            return syncCollection.get(id, GetOptions.getOptions().transcoder(transcoder)).contentAs(JsonObject.class);
        }
        catch(Exception e) {
            System.err.println("Error getting document: " + e.getMessage());
            return null;
        }
    }

    public long getCounter(String id) {
        long counter = 0l;
        try {
            counter = syncCollection.get(id).contentAs(Long.class);
        }
        catch (Exception ex) {
            System.out.println( "Issue getting counter, returning 0: " + id );
        }
        return counter;
    }

    public void incr(String key, int factor) {
        asyncCollection.binary().increment(key, IncrementOptions.incrementOptions().initial(factor).delta(factor));
    }

    public void incr(String key, int factor, int expiry) {
        asyncCollection.binary().increment(key, IncrementOptions.incrementOptions().initial(factor).delta(factor).expiry(Duration.ofSeconds(expiry)));
    }

    public void decr(String key, int factor) {
        asyncCollection.binary().decrement(key, DecrementOptions.decrementOptions().initial(0).delta(factor));
    }

    public void decr(String key, int factor, int expiry) {
        asyncCollection.binary().decrement(key, DecrementOptions.decrementOptions().initial(0).delta(factor).expiry(Duration.ofSeconds(expiry)));
    }

    public List<JsonObject> getDocumentsByQuery(String query, String collectionName) {
        QueryResult result = nerdPersistConfig.syncCluster.query(query, QueryOptions.queryOptions());
        List<JsonObject> list = new ArrayList<>();
        for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
            list.add(jsonParser.parse(row.get(collectionName).toString()).getAsJsonObject());
        }
        return list;
    }
}
