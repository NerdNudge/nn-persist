package com.neurospark.nerdnudge.couchbase.service;

import com.google.gson.JsonObject;

public interface NerdPersist {
    public void set( String id, int expiry, JsonObject data);

    public void set( String id, JsonObject data);

    public void delete( String id);

    public JsonObject get( String id );

    public long getCounter( String id );

    public void incr( String key, int factor);

    public void incr( String key, int factor, int expiry );

    public void decr( String key, int factor);

    public void decr( String key, int factor, int expiry );
}
