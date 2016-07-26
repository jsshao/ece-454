package ece454;

import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

import ece454.test.ExecutionLogger;

public class A3Client implements CuratorWatcher {
    String zkConnectString;
    String zkNode;
    int numThreads;
    int numOps;
    int keySpaceSize;
    CuratorFramework curClient;
    static Logger log;
    
    ExecutionLogger exlog;

    public static void main(String [] args) throws Exception {
	if (args.length != 5) {
	    System.err.println("Usage: java ece454.A3Client zkconnectstring zknode numthreads numops keyspacesize");
	    System.exit(-1);
	}

	BasicConfigurator.configure();
	log = Logger.getLogger(A3Client.class.getName());

	A3Client client = new A3Client(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));

	try {
	    client.start();
	    client.execute();
	} catch (Exception e) {
	    log.error("Uncaught exception", e);
	} finally {
	    client.stop();
	}
    }

    A3Client(String zkConnectString, String zkNode, int numThreads, int numOps, int keySpaceSize) {
	this.zkConnectString = zkConnectString; 
	this.zkNode = zkNode;
	this.numThreads = numThreads;
	this.numOps = numOps;
	this.keySpaceSize = keySpaceSize;

	exlog = new ExecutionLogger("execution.log");
    }

    void start() {
	curClient =
	    CuratorFrameworkFactory.builder()
	    .connectString(zkConnectString)
	    .retryPolicy(new RetryNTimes(10, 1000))
	    .connectionTimeoutMs(1000)
	    .sessionTimeoutMs(10000)
	    .build();

	curClient.start();
	exlog.start();
    }

    void execute() throws Exception {
	List<Thread> tlist = new ArrayList<>();
	List<MyRunnable> rlist = new ArrayList<>();
	for (int i = 0; i < numThreads; i++) {
	    MyRunnable r = new MyRunnable();
	    Thread t = new Thread(r);
	    tlist.add(t);
	    rlist.add(r);
	}
	long startTime = System.currentTimeMillis();
	for (int i = 0; i < numThreads; i++) {
	    tlist.get(i).start();
	}
	log.info("Done starting " + numThreads + " threads...");
	for (Thread t: tlist) {
	    t.join();
	}
	long estimatedTime = System.currentTimeMillis() - startTime;
	if (estimatedTime == 0)
	    estimatedTime++;
	float tput = 1000f * numOps * numThreads / estimatedTime;
	System.out.println("Aggregate throughput: " + tput + " ops/s");
	long totalLatency = 0;
	for (MyRunnable r: rlist) {
	    totalLatency += r.getTotalTime();
	}
	double avgLatency = (double)totalLatency / (numThreads * numOps) / 1000d;
	System.out.println("Average latency (ms): " + avgLatency);
    }

    void stop() {
	curClient.close();
	exlog.stop();
    }

    InetSocketAddress getPrimary() throws Exception {
	while (true) {
	    curClient.sync();
	    List<String> children =
		curClient.getChildren().usingWatcher(this).forPath(zkNode);
	    if (children.size() == 0) {
		log.error("No primary found");
		Thread.sleep(100);
		continue;
	    }
	    Collections.sort(children);
	    byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
	    String strData = new String(data);
	    String[] primary = strData.split(":");
	    log.info("Found primary " + strData);
	    return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
	}
    }

    KeyValueService.Client getThriftClient() {
	while (true) {
	    try {
		InetSocketAddress iad = getPrimary();
		TSocket sock = new TSocket(iad.getHostName(), iad.getPort());
		TTransport transport = new TFramedTransport(sock);
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		return new KeyValueService.Client(protocol);
	    } catch (Exception e) {
		log.error("Unable to connect to primary", e);
	    }
	    try {
		Thread.sleep(100);
	    } catch (InterruptedException e) {}
	}
    }

    public void process(WatchedEvent event) {
	log.info("ZooKeeper event " + event);
    }

    class MyRunnable implements Runnable {
	long totalTime;
	KeyValueService.Client client;
	MyRunnable() throws TException {
	    client = getThriftClient();
	}

	long getTotalTime() { return totalTime; }

	public void run() {
	    Random rand = new Random();
	    totalTime = 0;
	    long tid = Thread.currentThread().getId();
	    try {
		for (long i = 0; i < numOps; i++) {
		    String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
		    String value = "value-" + Math.abs(rand.nextLong());
		    long startTime = System.nanoTime();
		    //log.info("Starting operation at : " + startTime);
		    if (rand.nextBoolean()) {
			while (true) {
			    try {
				exlog.logWriteInvocation(tid, key, value);
				client.put(key, value);
				exlog.logWriteResponse(tid);
				break;
			    } catch (Exception e) {
				log.error("Exception during put", e);
				client = getThriftClient();
			    }
			}
		    } else {
			while (true) {
			    try {
				exlog.logReadInvocation(tid, key);
				String resp = client.get(key);
				exlog.logReadResponse(tid, resp);
				break;
			    } catch (Exception e) {
				log.error("Exception during get", e);
				client = getThriftClient();
			    }
			}
		    }
		    long diffTime = System.nanoTime() - startTime;
		    totalTime += diffTime / 1000;
		}
	    } catch (Exception x) {
		x.printStackTrace();
	    } 	
	}
    }
}
