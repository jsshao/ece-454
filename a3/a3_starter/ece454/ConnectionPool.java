package ece454;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.protocol.*;

public class ConnectionPool {
    private static int CONNECTION_LIMIT = 16;
    private static int POLL_FREQUENCY_MS = 1;
    private InetSocketAddress mAddr = null;

    private ConcurrentLinkedQueue<KeyValueService.Client> mPool = 
        new ConcurrentLinkedQueue<KeyValueService.Client>();

    public ConnectionPool(String addr) {
        int ind = addr.lastIndexOf(":");
        InetSocketAddress iad = new InetSocketAddress(
            addr.substring(0, ind),
            Integer.parseInt(addr.substring(ind+1, addr.length()))
        );
        mAddr = iad;
        for (int i = 0; i < CONNECTION_LIMIT; i++) {
            try {
                TSocket sock = new TSocket(iad.getHostName(), iad.getPort());
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                mPool.add(new KeyValueService.Client(protocol));
            } catch (Exception e) {
                System.out.println("Error creating connection pool");
            }
        }
    }

    public KeyValueService.Client getConnection() {
        while (mPool.isEmpty()) {
        } 
        KeyValueService.Client client = mPool.poll();
        return client;
    }

    public void releaseConnection(KeyValueService.Client client) {
        mPool.offer(client);
    }
}
