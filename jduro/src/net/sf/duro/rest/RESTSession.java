package net.sf.duro.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.PossrepObject;
import net.sf.duro.SimplePossrepObject;
import net.sf.duro.Tuple;

public class RESTSession extends DSession {

    private URL baseUrl;

    public RESTSession(URL url) {
        this.baseUrl = url;
    }

    public Object evaluate(String expr) {
        JsonStructure jsonStructure;
        try {
            URL url = new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(),
                    baseUrl.getFile() + '/' + expr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new DException(conn.getResponseMessage(), null);
            }

            JsonReader jsonReader = Json.createReader(new BufferedReader(
                    new InputStreamReader(conn.getInputStream())));
            jsonStructure = jsonReader.read();
            jsonReader.close();
        } catch (IOException e) {
            throw new DException(e);
        }

        switch (jsonStructure.getValueType()) {
        case ARRAY:
            return toArray((JsonArray) jsonStructure);
        case OBJECT:
            return toTuple((JsonObject) jsonStructure);
        default:
            throw new IllegalArgumentException("Invalid JSON structure: " + jsonStructure.getValueType());
        }
    }

    private static Tuple toTuple(JsonObject jsonObject) {
        Tuple tuple = new Tuple();
        for (String key: jsonObject.keySet()) {
            tuple.setAttribute(key, toJson(jsonObject.get(key)));
        }
        return tuple;
    }

    private static PossrepObject toScalar(String type, JsonObject jsonObject) {
        PossrepObject obj = new SimplePossrepObject(type);
        for (String key: jsonObject.keySet()) {
            obj.setProperty(key, toJson(jsonObject.get(key)));
        }
        return obj;
    }

    private static Object[] toArray(JsonArray jsonArray) {
        Object[] result = new Object[jsonArray.size()];
        for (int i = 0; i < jsonArray.size(); i++) {
            result[i] = toJson(jsonArray.get(i));
        }
        return result;
    }

    private static Object toJson(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
        case ARRAY:
            return toArray((JsonArray) jsonValue);
        case OBJECT:
            JsonString jtype = ((JsonObject) jsonValue).getJsonString("@type");
            if (jtype != null) {
                return toScalar(jtype.getString(), (JsonObject) jsonValue);
            }
            return toTuple((JsonObject) jsonValue);
        case STRING:
            return ((JsonString) jsonValue).getString();
        case NUMBER:
            return Double.valueOf(((JsonNumber) jsonValue).doubleValue());
        case FALSE:
            return Boolean.FALSE;
        case TRUE:
            return Boolean.TRUE;
        case NULL:
            return null;
        default:
            throw new IllegalArgumentException("Invalid JSON value: " + jsonValue.getValueType());
        }
    }

    @Override
    public void execute(String code) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evaluate(String expr, Object dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T evaluate(String expr, Class<T> destClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setVar(String name, Object v) {
        throw new UnsupportedOperationException();
    }
}
