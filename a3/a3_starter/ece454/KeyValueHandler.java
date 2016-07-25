package ece454;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private HashSet<String> pool = new HashSet<String>(); 
    private boolean snapshotted = false;
    private Object backup_lock = new Object();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();	
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        if (isPrimary()) {
            try {
                lock(key);
                String ret = myMap.get(key);
                if (ret == null)
                    return "";
                else
                    return ret;
            } finally {
                unlock(key);
            } 
        } else {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        if (isPrimary() && getBackup() != null) {
            try {
                lock(key);
                myMap.put(key, value);
                KeyValueService.Client client = getThriftClient();
                while (true) {
                    try {
                        client.put(key, value);
                        break;
                    } catch (Exception e) {
                        client = getThriftClient();
                    }
                }
            } finally {
                unlock(key);
            }
        } else {
            myMap.put(key, value);
        }
    }

    private void lock(String name) {
        synchronized(pool) {
            while(pool.contains(name)) {
                try {
                    pool.wait();
                } catch (Exception e) {
                }
            }
            pool.add(name);
        }
    }

    private void unlock(String name) {
        synchronized(pool) {
            pool.remove(name);
            pool.notifyAll();
        }
    }

    private KeyValueService.Client getThriftClient() {
        return getThriftClient(getBackup());
    }

    private KeyValueService.Client getThriftClient(String backup) {
        while (true) {
            try {
                int ind = backup.lastIndexOf(":");
                InetSocketAddress iad = new InetSocketAddress(
                          backup.substring(0, ind),
                          Integer.parseInt(backup.substring(ind+1, backup.length()))
                );
                TSocket sock = new TSocket(iad.getHostName(),iad.getPort());
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                return new KeyValueService.Client(protocol);
            } catch (Exception e) {
                System.out.println("Unable to connect to backup");
            }
        }
    }

    private String getPrimary() {
        try {
            GetChildrenBuilder childrenBuilder = curClient.getChildren();
            List<String> children = childrenBuilder.watched().forPath(zkNode);
            String primary = null;
            String primaryChild = null;
            for (String child : children) {
                if (primaryChild == null || child.compareTo(primaryChild) < 0) {
                    primary = new String(curClient.getData().forPath(zkNode + "/" + child));
                }
            }
            return primary;
        } catch (Exception e) {
            System.out.println("Cannot get primary");
            return null;
        }
    }

    private boolean isPrimary() {
        return host + ":" + port == getPrimary();
    }

    private String getBackup() {
        synchronized(backup_lock) {
            try {
                GetChildrenBuilder childrenBuilder = curClient.getChildren();
                List<String> children = childrenBuilder.watched().forPath(zkNode);
                String primary = getPrimary();
                for (String child : children) {
                    String addr = new String(curClient.getData().forPath(zkNode + "/" + child));
                    if (addr != primary) {
                        // First time seeing new backup. Copy snapshot
                        if (!snapshotted) {
                            snapshotted = true;
                            KeyValueService.Client client = getThriftClient(addr);
                            for (String key: myMap.keySet()) {
                                client.put(key, myMap.get(key));
                            }                            
                        }
                        return addr;
                    }
                }
                return null;
            } catch (Exception e) {
                System.out.println("Cannot get backup");
                return null;
            }
        }
    }
}
