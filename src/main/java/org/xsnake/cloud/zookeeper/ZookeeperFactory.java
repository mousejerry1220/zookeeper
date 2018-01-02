package org.xsnake.cloud.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperFactory {

	@Autowired
	ZookeeperConfiguration zookeeperConfiguration;
	
	private static final Logger LOG;
	static {
	    LOG = LoggerFactory.getLogger(ZookeeperFactory.class);
	}
	
	@Bean	
	public ZooKeeper zooKeeper() throws IOException, TimeoutException{
		long start = System.currentTimeMillis();
		Long timeout = zookeeperConfiguration.getTimeout();
		ZooKeeper zk = new ZooKeeper(zookeeperConfiguration.getUrl(),  timeout.intValue(), new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		//开启超时判断线程，如果连接成功或者连接超时则停止阻塞
		CountDownLatch timeoutCountDownLatch = new CountDownLatch(1);
		new Thread(){
			public void run() {
				while(true){
					if( (System.currentTimeMillis() - start > timeout) || zk.getState() == States.CONNECTED){
						timeoutCountDownLatch.countDown();
						break;
					}
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			};
		}.start();
		
		//阻塞等待
		try {
			timeoutCountDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//阻塞完成后判断ZK有没有连接成功，没有连接成功则抛出错误
		if(zk.getState() != States.CONNECTED){
			LOG.error("连接Zookeeper超时");
			throw new TimeoutException("连接超时");
		}
		
		LOG.info("连接Zookeeper成功");
		return zk;
	}
	
}
