package org.xsnake.cloud.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ZookeeperTemplate {

	@Autowired
	protected ZooKeeper zooKeeper;
	
	/**
	 * 创建临时目录
	 * @param node
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void $node(String node) throws IOException {
		node(node, null, CreateMode.EPHEMERAL);
	}
	
	/**
	 * 创建临时目录及数据
	 * @param node
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void $node(String node,byte[] data) throws IOException {
		node(node, data, CreateMode.EPHEMERAL);
	}

	/**
	 * 创建目录
	 * @param node
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void node(String node) throws IOException {
		node(node, null, CreateMode.PERSISTENT);
	}

	/**
	 * 创建目录及数据
	 * @param node
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void node(String node,byte[] data) throws IOException {
		node(node, data, CreateMode.PERSISTENT);
	}
	
	/**
	 * 创建目录及数据，如果目录存在，则更新数据
	 * @param path
	 * @param value
	 * @param createMode
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void node(String path,byte[] value,CreateMode createMode) throws IOException{
		byte[] data = value != null ? value : null;
		try{
			if(zooKeeper.exists(path, null)==null){
				zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
			}else{
				zooKeeper.setData(path, data, -1);
			}
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	/**
	 * 获取目录数据
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public byte[] data(String path) throws IOException{
		try{
			return zooKeeper.getData(path, null, null);
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	/**
	 * 判断是否存在
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean exists(String path) throws IOException{
		try{
			return zooKeeper.exists(path, null)!=null;
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	public void delete(String path) throws IOException{
		try{
			zooKeeper.delete(path, -1);
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	public void onChange(String path,Watcher watcher) throws IOException{
		try{
			zooKeeper.getChildren(path, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					try {
						watcher.process(event);
						zooKeeper.getChildren(path, this);
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	public List<String> list(String path) throws IOException{
		try{
			return zooKeeper.getChildren(path,null);
		}catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
}
