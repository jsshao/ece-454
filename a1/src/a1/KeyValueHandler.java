package a1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.async.*;


class MultiGetCallback implements AsyncMethodCallback<KeyValueService.AsyncClient.multiGet_call> {
    private List<Integer> mIds;
    private Map<Integer, ByteBuffer> mRetMap;
    private CountDownLatch mLatch;
    private Integer mServer; 
    private KeyValueService.AsyncClient mClient;
    private ConnectionPool mPool;

    public MultiGetCallback(List<Integer> ids, Map<Integer, ByteBuffer> retMap, CountDownLatch latch,
            ConnectionPool pool, Integer server, KeyValueService.AsyncClient client) {
        mIds = ids;
        mRetMap = retMap;
        mLatch = latch;
        mServer = server;
        mClient = client;
        mPool = pool;
    }

    public void onComplete(KeyValueService.AsyncClient.multiGet_call c) {
        try {
            List<ByteBuffer> ret = c.getResult();
            for (int i=0; i<mIds.size(); i++) {
                mRetMap.put(mIds.get(i), ret.get(i));
            }
        } catch (TException e) {
            e.printStackTrace();
        }
        mLatch.countDown();
        mPool.releaseConnection(mServer, mClient);
    }

    public void onError(Exception e) {
        e.printStackTrace();
        mLatch.countDown();
        mPool.releaseConnection(mServer, mClient);
    }
}

class MultiPutCallback implements AsyncMethodCallback<KeyValueService.AsyncClient.multiPut_call> {
    private List<Integer> mIds;
    private Map<Integer, ByteBuffer> mRetMap;
    private CountDownLatch mLatch;
    private Integer mServer; 
    private KeyValueService.AsyncClient mClient;
    private ConnectionPool mPool;
    
    public MultiPutCallback(List<Integer> ids, Map<Integer, ByteBuffer> retMap, CountDownLatch latch,
            ConnectionPool pool, Integer server, KeyValueService.AsyncClient client) {
        mIds = ids;
        mRetMap = retMap;
        mLatch = latch;
        mServer = server;
        mClient = client;
        mPool = pool;
    }
     
    public void onComplete(KeyValueService.AsyncClient.multiPut_call c) {
        try {
            List<ByteBuffer> ret = c.getResult();
            for (int i=0; i<mIds.size(); i++) {
                mRetMap.put(mIds.get(i), ret.get(i));
            }
        } catch (TException e) {
            e.printStackTrace();
        }
        mLatch.countDown();
        mPool.releaseConnection(mServer, mClient);
    }

    public void onError(Exception e) {
        e.printStackTrace();
        mLatch.countDown();
        mPool.releaseConnection(mServer, mClient);
    }
}

public class KeyValueHandler implements KeyValueService.Iface {
    private ConcurrentHashMap<String, ByteBuffer> map = new ConcurrentHashMap<String, ByteBuffer>();
    private HashMap<Integer, String> mHosts;
    private HashMap<Integer, Integer> mPorts;
    private ConnectionPool mPool;
    private int mServerId;
    private int mNumOfServers;

    public KeyValueHandler(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports, int serverId) {
        mHosts = hosts;
        mPorts = ports; 
        mPool = new ConnectionPool(hosts, ports);
        mNumOfServers = hosts.size();
        mServerId = serverId;
    }

    public List<String> getGroupMembers() {
        List<String> ret = new ArrayList<String>();
        ret.add("jsshao");
        ret.add("mhlu");
        return ret;
    }

    public List<ByteBuffer> multiGet(List<String> keys) {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>(keys.size()); 
        HashMap<Integer, ArrayList<String>> batches = new HashMap<Integer, ArrayList<String>>();
        HashMap<Integer, ArrayList<Integer>> keyIds = new HashMap<Integer, ArrayList<Integer>>();
        ConcurrentHashMap<Integer, ByteBuffer> retMap = new ConcurrentHashMap<Integer, ByteBuffer>();
        byte empty[] = {};

        for (int i=0; i<keys.size(); i++) {
            String key = keys.get(i);
            int expectedServer = ((key.hashCode() % mNumOfServers) + mNumOfServers) % mNumOfServers;

            if ( expectedServer == mServerId ) {
                retMap.put(i, map.containsKey(key) ? map.get(key) : ByteBuffer.wrap(empty));
                continue;
            }

            if ( !batches.containsKey(expectedServer) ) {
                batches.put(expectedServer, new ArrayList<String>());
                keyIds.put(expectedServer, new ArrayList<Integer>());
            }
            List<String> batch = batches.get(expectedServer);
            List<Integer> ids = keyIds.get(expectedServer);
            batch.add(key);
            ids.add(i);
        }

        CountDownLatch latch = new CountDownLatch(batches.size());
        for (Map.Entry<Integer, ArrayList<String>> entry : batches.entrySet()) {
            Integer server = entry.getKey();
            List<String> batch = entry.getValue();
            List<Integer> ids = keyIds.get(server);
            getRemote(batch, server, ids, retMap, latch);
        }
    
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i=0; i<keys.size(); i++) {
            values.add(retMap.get(i));
            if (retMap.get(i) == null)
                System.out.println(keys.size() + "WTF" + retMap.size());
        }

        return values;
    }

    public List<ByteBuffer> multiPut(List<String> keys, List<ByteBuffer> values) throws IllegalArgument {
        if (keys.size() != values.size()) {
            throw new IllegalArgument("List of keys does not have same length as list of values");
        }

        List<ByteBuffer> oldValues = new ArrayList<ByteBuffer>(keys.size());
        HashMap<Integer, ArrayList<String>> keyBatches = new HashMap<Integer, ArrayList<String>>();
        HashMap<Integer, ArrayList<ByteBuffer>> valueBatches = new HashMap<Integer, ArrayList<ByteBuffer>>();
        HashMap<Integer, ArrayList<Integer>> keyIds = new HashMap<Integer, ArrayList<Integer>>();
        ConcurrentHashMap<Integer, ByteBuffer> retMap = new ConcurrentHashMap<Integer, ByteBuffer>();
        byte empty[] = {};

        for (int i=0; i<keys.size(); i++) {
            String key = keys.get(i);
            ByteBuffer value = values.get(i);
            int expectedServer = ((key.hashCode() % mNumOfServers) + mNumOfServers) % mNumOfServers;

            if ( expectedServer == mServerId ) {
                retMap.put(i, map.containsKey(key) ? map.get(key) : ByteBuffer.wrap(empty));
                map.put(key, value);
                continue;
            }

            if ( !keyBatches.containsKey(expectedServer) ) {
                keyBatches.put(expectedServer, new ArrayList<String>());
                valueBatches.put(expectedServer, new ArrayList<ByteBuffer>());
                keyIds.put(expectedServer, new ArrayList<Integer>());
            }
            List<String> keyBatch = keyBatches.get(expectedServer);
            List<ByteBuffer> valueBatch = valueBatches.get(expectedServer);
            List<Integer> ids = keyIds.get(expectedServer);
            keyBatch.add(key);
            valueBatch.add(value);
            ids.add(i);
        }

        CountDownLatch latch = new CountDownLatch(keyBatches.size());
        for (Map.Entry<Integer, ArrayList<String>> entry : keyBatches.entrySet()) {
            Integer server = entry.getKey();
            List<String> keyBatch = entry.getValue();
            List<ByteBuffer> valueBatch = valueBatches.get(server);
            List<Integer> ids = keyIds.get(server);
            putRemote(keyBatch, valueBatch, server, ids, retMap, latch);
        }

        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i=0; i<keys.size(); i++) {
            oldValues.add(retMap.get(i));
            if (retMap.get(i) == null)
                System.out.println(keys.size() + "WTF" + retMap.size());
        }
        return oldValues;
    }

    private void getRemote(List<String> keys, Integer server,
            List<Integer> ids, Map<Integer, ByteBuffer> retMap, CountDownLatch latch) {
        /*
        System.out.print("Making remote get call to " + server + " for ( ");
        for(String s: keys) System.out.print(s + " ");
        System.out.println(" )");
        */
        try {
            KeyValueService.AsyncClient client = mPool.getConnection(server);
            MultiGetCallback callback = new MultiGetCallback(ids, retMap, latch, mPool, server, client);
            client.multiGet(keys, callback);
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private void putRemote(List<String> keys, List<ByteBuffer> values, Integer server,
            List<Integer> ids, Map<Integer, ByteBuffer> retMap, CountDownLatch latch) {
        /*
        System.out.print("Making remote put call to " + server + " for ( ");
        for(String s: keys) System.out.print(s + " ");
        System.out.println(" )");
        */
        try {
            KeyValueService.AsyncClient client = mPool.getConnection(server);
            MultiPutCallback callback = new MultiPutCallback(ids, retMap, latch, mPool, server, client);
            client.multiPut(keys, values, callback);
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
