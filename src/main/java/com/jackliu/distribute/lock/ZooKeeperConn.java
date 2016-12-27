package com.jackliu.distribute.lock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jackliu on 16/12/27.
 */
public class ZooKeeperConn implements Watcher{

    private static final Logger LOG = Logger.getLogger(ZooKeeperConn.class);

    protected ZooKeeper zk = null;
    private AtomicBoolean connected = new AtomicBoolean(false);
    private int sessionTimeOut = 10000;
    private long sessionId = 0L;
    private long retryDelay = 500L;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private String hosts;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZooKeeperConn(String hosts,int sessionTimeOut){
        this.hosts = hosts;
        this.sessionTimeOut = sessionTimeOut;
    }

    public ZooKeeper getZk(){
        return zk;
    }

    public void connect() throws IOException,InterruptedException{
        connect(0l);
    }

    private void connect(long sessionId) throws IOException,InterruptedException{
        try {
            if (sessionId != 0l)
                zk = new ZooKeeper(hosts, sessionTimeOut,this, sessionId, null);
            else
                zk = new ZooKeeper(hosts, sessionTimeOut,this);
        } catch (IOException e) {
            LOG.error("Could not connect to ZK, network failure, retry.", e);
            throw e;
        }
        try {
            connectedSignal.await(sessionTimeOut, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Zookeeper connect signal is interrupted.");
            throw e;
        }
    }

    private void disConnect() throws InterruptedException{
        try {
            connected.set(false);
            zk.close();
        } catch (InterruptedException e) {
            LOG.error("Can not disconnect to zk.", e);
            throw e;
        }
    }

    public ZooKeeperConn(String hosts){
        this(hosts,10000);
    }

    @Override
    public void process(WatchedEvent event){
        switch (event.getState()) {
            case SyncConnected:
                this.sessionId = zk.getSessionId();
                connected.set(true);
                connectedSignal.countDown();
                LOG.info("Successfully connected to ZK.");
                break;
            case Expired:
                LOG.error("Detected conn to zk session expired. retry conn");
                do {
                    connectedSignal = new CountDownLatch(1);
                    try {
                        disConnect();
                        connect();
                        break;
                    } catch (InterruptedException e) {
                        //TO-DO add alert message
                        LOG.error("Can not reconnect to zk.", e);
                    }catch (IOException e){
                        LOG.error("Connect to zookeeper error.",e);
                    }
                    retryDelay(2);
                } while (true);
                break;
            case Disconnected:
                LOG.error("Lost connection to ZK. Disconnected, will auto reconnected until success.");
                do {
                    connectedSignal = new CountDownLatch(1);
                    try {
                        disConnect();
                        connect(this.sessionId);
                        break;
                    } catch (InterruptedException e) {
                        //TO-DO add alert message
                        LOG.error("Can not reconnect to zk.", e);
                    }catch (IOException e){
                        LOG.error("Connect to zookeeper error.",e);
                    }
                    retryDelay(2);
                } while (true);
                break;
            default:
                break;
        }
    }

    protected void retryDelay(int attemptCount) {
        if (attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * retryDelay);
            } catch (InterruptedException e) {
                LOG.debug("Failed to sleep: " + e, e);
            }
        }
    }
}
