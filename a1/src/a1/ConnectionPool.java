package a1;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
import java.io.IOException;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.async.*;

public class ConnectionPool {
    private static int CONNECTION_LIMIT = 16;
    private static int POLL_FREQUENCY_MS = 1;
    private HashMap<Integer, String> mHosts;
    private HashMap<Integer, Integer> mPorts;
    private HashMap<Integer, ConcurrentLinkedQueue<KeyValueService.AsyncClient>> mPool;
    private ConcurrentHashMap<Integer, Integer> mNumExistingConnections;

    public ConnectionPool(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports) {
        mHosts = hosts;
        mPorts = ports;
        mPool = new HashMap<Integer, ConcurrentLinkedQueue<KeyValueService.AsyncClient>>();
        mNumExistingConnections = new ConcurrentHashMap<Integer, Integer>();
        for (Integer serverId : hosts.keySet()) {
            mNumExistingConnections.put(serverId, 0);
            mPool.put(serverId, new ConcurrentLinkedQueue<KeyValueService.AsyncClient>());
        }
    }

    public KeyValueService.AsyncClient getConnection(int serverId) {
        ConcurrentLinkedQueue<KeyValueService.AsyncClient> queue = mPool.get(serverId);
        if (queue == null) {
            System.out.println("Tried to get queue for " + serverId);
            return null;
        }
        synchronized(queue) {
            if (queue.isEmpty() && mNumExistingConnections.get(serverId) < CONNECTION_LIMIT) {
                //System.out.println("Creating new connection");
                mNumExistingConnections.put(serverId, mNumExistingConnections.get(serverId) + 1);
                return createNewClient(serverId);
            } else {
                //System.out.println("Waiting for connection to be free");
                while (queue.isEmpty()) {
                    /*
                    try {
                        Thread.sleep(POLL_FREQUENCY_MS);
                    } catch (InterruptedException x) {
                    }
                    */
                }
                KeyValueService.AsyncClient client = queue.poll();
                return client;
            }
        }
    }

    public void releaseConnection(Integer serverId, KeyValueService.AsyncClient client) {
        ConcurrentLinkedQueue<KeyValueService.AsyncClient> queue = mPool.get(serverId);
        queue.offer(client);
        //System.out.println("Putting client back into queue");
    }

    private KeyValueService.AsyncClient createNewClient(int serverId) {
        //System.out.println("Creating new client with server " + serverId);
        try {
            /*TSocket sock = new TNonblockingSocket(mHosts.get(serverId), mPorts.get(serverId));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            transport.open();*/
            TNonblockingTransport transport = new TNonblockingSocket(mHosts.get(serverId), mPorts.get(serverId));
            TProtocolFactory pf = new TBinaryProtocol.Factory();
            TAsyncClientManager cm = new TAsyncClientManager();
            KeyValueService.AsyncClient client = new KeyValueService.AsyncClient(pf, cm, transport);
            return client;
        //} catch (TException x) {
        //    x.printStackTrace();
        } catch (IOException e) {
                e.printStackTrace();
        }
        return null;
    }
}
