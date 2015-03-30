package com.yahoo.ycsb.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * @author  dayasaktikartika on 30/3/15.
 */
public class SpymemcachedClient extends DB {

    public static final int NO_EXPIRE = 0;
    public static final int SUCCESS = 0;
    public static final int ERROR_UNHANDLED_EXCEPTION = -1;
    public static final int ERR_UNSUPPORTED_OPERATION = -2;
    public static final int ERR_OPS_FAILED = -3;
    public static final int ERR_NOT_FOUND = -4;
    public static final int ERR_UNSUPPORTED_STORED_OBJECT = -5;

    public static final String PROP_KEY_SERVER_ENDPOINTS="spymemcached.serverEndpoints";
    public static final String PROP_KEY_UPDATES_USES_UPSERTS="spymemcached.updatesUsesUpserts";
    public static final String PROP_KEY_INSERTS_USES_UPSERTS="spymemcached.insertsUsesUpserts";
    public static final String PROP_KEY_ROOT_NS="spymemcached.rootNS";

    private MemcachedClient client = null;
    private boolean updatesUsesUpserts = true;
    private boolean insertsUsesUpserts = true;
    private String rootNS = "ycsb";

    @Override
    public void init() throws DBException {
        final String[] socketAddressesStr = getProperties().getProperty(PROP_KEY_SERVER_ENDPOINTS,"localhost:11211").split(",");
        final boolean updatesUsesUpserts = Boolean.parseBoolean(getProperties().getProperty(PROP_KEY_UPDATES_USES_UPSERTS, "true"));
        final boolean insertsUsesUpserts = Boolean.parseBoolean(getProperties().getProperty(PROP_KEY_INSERTS_USES_UPSERTS, "true"));
        final String rootNS = getProperties().getProperty(PROP_KEY_ROOT_NS, "ycsb");
        System.out.println("socketAddressesStr=[" + Arrays.asList(socketAddressesStr) + "]");
        System.out.println("updatesUsesUpserts=[" + updatesUsesUpserts + "]");
        System.out.println("insertsUsesUpserts=[" + insertsUsesUpserts + "]");
        System.out.println("rootNS=[" + rootNS + "]");
        final InetSocketAddress[] inetSocketAddresses = new InetSocketAddress[socketAddressesStr.length];
        int i = 0;
        for (final String sockAddr : socketAddressesStr){
            final int colonIndex = sockAddr.lastIndexOf(':');
            final String host = sockAddr.substring(0,colonIndex);
            final String portStr = sockAddr.substring(colonIndex+1);
            inetSocketAddresses[i] = InetSocketAddress.createUnresolved(host,Integer.parseInt(portStr));
            i++;
        }
        try{
            final MemcachedClient client = new MemcachedClient(inetSocketAddresses);
            this.updatesUsesUpserts = updatesUsesUpserts;
            this.insertsUsesUpserts = insertsUsesUpserts;
            this.client = client;
            this.rootNS = rootNS;
            System.out.println("init successful");
        }catch(Exception e){
            System.err.println("Init failed");
            e.printStackTrace();
            throw new DBException(e);
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (this.client != null){
            this.client.shutdown();
        }
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        final String memcachedKey = getMemcachedKey(table,key);
        try{
            final MemcachedClient client = getClient();
            final Object value = client.get(memcachedKey);
            if (value == null){
                return ERR_NOT_FOUND;
            }
            if (value instanceof String){
                populateFromJsonString((String) value, result);
                return SUCCESS;
            }
            System.err.println("Unsupported value for key [" + memcachedKey + "]=>[" + value + "]" );
            return ERR_UNSUPPORTED_STORED_OBJECT;
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("Exception on delete key [" + memcachedKey + "]" );
            return ERROR_UNHANDLED_EXCEPTION;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return ERR_UNSUPPORTED_OPERATION;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        if (useUpsertForUpdates()){
            return upsert(table,key,values);
        }
        return updateOps(table,key,values);
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (useUpsertForInserts()){
            return upsert(table,key,values);
        }
        return insertOps(table,key,values);
    }

    private int upsert(String table, String key, HashMap<String, ByteIterator> values){
        final String memcachedKey = getMemcachedKey(table,key);
        try{
            final MemcachedClient client = getClient();
            final OperationFuture<Boolean> futOps = client.set(key, NO_EXPIRE, toJsonNode(values));
            return futOps.get() ? SUCCESS : ERR_OPS_FAILED;
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("Exception on upsert key [" + memcachedKey + "]" );
            return ERROR_UNHANDLED_EXCEPTION;
        }
    }

    private int insertOps(String table, String key, HashMap<String, ByteIterator> values){
        final String memcachedKey = getMemcachedKey(table,key);
        try{
            final MemcachedClient client = getClient();
            final OperationFuture<Boolean> futOps = client.add(key, NO_EXPIRE, toJsonNode(values));
            return futOps.get() ? SUCCESS : ERR_OPS_FAILED;
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("Exception on add key [" + memcachedKey + "]" );
            return ERROR_UNHANDLED_EXCEPTION;
        }
    }

    private int updateOps(String table, String key, HashMap<String, ByteIterator> values){
        final String memcachedKey = getMemcachedKey(table,key);
        try{
            final MemcachedClient client = getClient();
            final OperationFuture<Boolean> futOps = client.replace(key, NO_EXPIRE, toJsonNode(values));
            return futOps.get() ? SUCCESS : ERR_OPS_FAILED;
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("Exception on replace key [" + memcachedKey + "]" );
            return ERROR_UNHANDLED_EXCEPTION;
        }
    }

    public static String toJsonNode(Map<String,ByteIterator> fieldValues) throws JsonProcessingException{
        final ObjectNode objectNode = objectMapper.createObjectNode();
        for (Map.Entry<String,ByteIterator> f : fieldValues.entrySet()){
            objectNode.put(f.getKey(),f.getValue().toString());
        }
        return objectNode.toString();
    }

    public static void populateFromJsonString(String jsonFieldValues, Map<String, ByteIterator> result) throws IOException{
        final JsonNode jsonNode = objectMapper.readTree(jsonFieldValues);
        final Iterator<Map.Entry<String,JsonNode>> entryIterator = jsonNode.fields();
        while(entryIterator.hasNext()){
            final Map.Entry<String,JsonNode> e = entryIterator.next();
            result.put(e.getKey(), new StringByteIterator(e.getValue().textValue()));
        }
    }


    @Override
    public int delete(String table, String key) {
        final String memcachedKey = getMemcachedKey(table,key);
        try{
            final MemcachedClient client = getClient();
            final OperationFuture<Boolean> futOps = client.delete(memcachedKey);
            futOps.get();
            return SUCCESS;
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Exception on delete key [" + memcachedKey + "]" );
            return ERROR_UNHANDLED_EXCEPTION;
        }
    }

    protected String getMemcachedKey(String table,String key){
        return getRootNS() + ":" + table + ":" + key;
    }

    protected MemcachedClient getClient(){
        if (this.client == null){
            throw new IllegalStateException("client not initialized");
        }
        return this.client;
    }

    protected String getRootNS(){
        return this.rootNS;
    }

    protected boolean useUpsertForInserts(){
        return this.insertsUsesUpserts;
    }

    protected boolean useUpsertForUpdates(){
        return this.updatesUsesUpserts;
    }

}
