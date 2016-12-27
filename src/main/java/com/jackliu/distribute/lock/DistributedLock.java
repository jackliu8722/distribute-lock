/**
 * @Probject Name: oms-core
 * @Path: test.com.wangfj.order.controllerDistributedLock.java
 * @Create By kong
 * @Create In 2015年8月3日 下午4:50:18
 *
 */
package com.jackliu.distribute.lock;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 分布式锁
 * @author Jackliu
 * @date 2016-12-26
 *
 */
public class DistributedLock implements Lock,Watcher{
    private ZooKeeperConn zkConn;
    private String root = "/locks";//根
    private String tempParentNode;
    private String lockName;//竞争资源的标志
    private String waitNode;//等待前一个锁
    private String tempNodeName;//当前锁
    private CountDownLatch latch;//计数器
    private List<Exception> exception = new ArrayList<Exception>();
    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);
    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     * @param lockName 竞争资源标志,lockName中不能包含单词lock
     * @param zkConn Zookeeper连接管理
     */
    public DistributedLock(String lockName,ZooKeeperConn zkConn){
        this.lockName = lockName;
        this.zkConn = zkConn;
        // 创建一个与服务器的连接
         try {
            Stat stat = zkConn.getZk().exists(root, false);
            if(stat == null){
            	//创建根节点
                zkConn.getZk().create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            Stat stat1 = zkConn.getZk().exists(root+"/"+lockName, false);
            if(stat1 == null){
            	//创建临时根节点
            	try{
            		zkConn.getZk().create(root + "/" + lockName,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            	}catch(Exception e){
            		Stat stat2 = zkConn.getZk().exists(root+"/"+lockName, false);
            		if(stat2 == null){
            			throw new RuntimeException(e);
            		}
            	}
            	
            }
            tempParentNode = root+"/"+lockName;
        } catch (KeeperException e) {
            exception.add(e);
        } catch (InterruptedException e) {
            exception.add(e);
        }
    }

    public void lock() {
        if(exception.size() > 0){
            throw new LockException(exception.get(0));
        }
        try {
            if(this.tryLock()){
            	logger.info("Thread " + Thread.currentThread().getId() + " " +tempNodeName + " get lock true");
                return;
            }else{
               waitForLock(waitNode, -1);//等待锁
            }
        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        } 
    }
 
    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if(lockName.contains(splitStr))
                throw new LockException("lockName can not contains \\u000B");
            
            //创建临时子节点
            tempNodeName = zkConn.getZk().create(tempParentNode + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info(tempNodeName + " is created ");
            //取出所有子节点
            List<String> subNodes = zkConn.getZk().getChildren(tempParentNode, false);
            //取出所有lockName的锁
            Collections.sort(subNodes);
            if(tempNodeName.equals(tempParentNode+"/"+subNodes.get(0))){
                //如果是最小的节点,则表示取得锁
                return true;
            }
            //如果不是最小的节点，找到比自己小1的节点
            String subZnode = tempNodeName.substring(tempNodeName.lastIndexOf("/") + 1);
            waitNode = subNodes.get(Collections.binarySearch(subNodes, subZnode) - 1);
        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
        try {
            if(this.tryLock()){
                return true;
            }
            return waitForLock(waitNode,unit.toMillis(time));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
 
    private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
        Stat stat = zkConn.getZk().exists(tempParentNode+"/" + lower,this);
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        if(stat != null){
            logger.info("Thread " + Thread.currentThread().getId() + " waiting for " + tempParentNode + "/" + lower);
            this.latch = new CountDownLatch(1);
            if(waitTime > 0) {
                boolean flag = this.latch.await(waitTime, TimeUnit.MILLISECONDS);
                this.latch = null;
                return flag;
            }else {
                this.latch.await();
                return true;
            }
        }
        return true;
    }
 
    public void unlock() {
        try {
            logger.info("unlock " + tempNodeName);
            zkConn.getZk().delete(tempNodeName,-1);
            tempNodeName = null;
            List<String> subNodes = zkConn.getZk().getChildren(tempParentNode, false);
            if(subNodes.isEmpty()){
            	zkConn.getZk().delete(tempParentNode,-1);
            }
           
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
 
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("开始监听");

        if(event.getPath()!=null){
            if (event.getState() == KeeperState.SyncConnected){
                if (EventType.NodeDeleted == event.getType()){
                    if(event.getPath().equalsIgnoreCase(tempParentNode+"/"+waitNode)){
                        if(this.latch != null) {
                            this.latch.countDown();
                        }
                    }
                }
            }
        }

    }

    public Condition newCondition() {
        return null;
    }
     
    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }
    
  
    
    public static void main(String[] args) throws IOException, InterruptedException {
        DOMConfigurator.configure("conf/log4j.xml");
        ZooKeeperConn zkConn = new ZooKeeperConn("127.0.0.1:2181");
        zkConn.connect();

        test2(zkConn);
	}

    public static void test2(ZooKeeperConn zkConn) throws InterruptedException {
        Thread t1 = new Thread(){
            public void run(){
                DistributedLock lock = null;
                try{
                    lock = new DistributedLock("test2",zkConn);
                    lock.lock();
                    System.out.println(Thread.currentThread().getName() + "  get lock");
                }catch (Exception e){

                }
            }
        };
        t1.start();

        try {
            Thread.sleep(1000);
        }catch (InterruptedException e){

        }

        Thread t2 = new Thread(){
            public void run(){
                DistributedLock lock = null;
                try{
                    System.out.println(Thread.currentThread().getName() + "  geting lock");
                    lock = new DistributedLock("test2",zkConn);
                    if(lock.tryLock(10,TimeUnit.SECONDS)){
                        System.out.println(Thread.currentThread().getName() + "  get lock succ");
                    }else{
                        System.out.println(Thread.currentThread().getName() + "  get lock fail");
                    }
                }catch (Exception e){

                }
            }
        };
        t2.start();
        Thread.currentThread().join();
    }

    public static void test1(ZooKeeperConn zkConn) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(20);

        for(int i =0;i<20;i++){
            Thread t = new Thread(){
                public void run(){
                    DistributedLock lock = null;
                    try {
                        lock = new DistributedLock("test1",zkConn);
                        lock.lock();
                        System.out.println(Thread.currentThread().getName() + "  get lock");
                    }catch (Exception e) {
                        e.printStackTrace();
                    }finally {
                        System.out.println(Thread.currentThread().getName() + "  unlock lock");
                        lock.unlock();
                        latch.countDown();
                    }
                }
            };
            t.setName("Lock-thread-" + i);
            t.start();
        }
        latch.await();
    }
}
