package com.neurospark.nerdnudge.couchbase.utils;

import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;

public class GsonSerializer implements JsonSerializer {

    private final Gson gson = new Gson();

    @Override
    public byte[] serialize(Object input) {
        String json = gson.toJson(input);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T deserialize(Class<T> target, byte[] input) {
        String str = new String(input, StandardCharsets.UTF_8);
        return gson.fromJson(str, target);
    }

    @Override
    public <T> T deserialize(TypeRef<T> target, byte[] input) {
        String str = new String(input, StandardCharsets.UTF_8);
        return gson.fromJson(str, target.type());
    }
}
