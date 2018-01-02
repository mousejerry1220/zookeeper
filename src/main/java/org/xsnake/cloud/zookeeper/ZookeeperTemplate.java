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
	public void tempDir(String node) throws KeeperException, InterruptedException, IOException {
		dir(node, null, CreateMode.EPHEMERAL);
	}
	
	/**
	 * 创建临时目录及数据
	 * @param node
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void tempDir(String node,String data) throws KeeperException, InterruptedException, IOException {
		dir(node, data, CreateMode.EPHEMERAL);
	}

	/**
	 * 创建目录
	 * @param node
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void dir(String node) throws KeeperException, InterruptedException, IOException {
		dir(node, null, CreateMode.PERSISTENT);
	}

	/**
	 * 创建目录及数据
	 * @param node
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void dir(String node,String data) throws KeeperException, InterruptedException, IOException {
		dir(node, data, CreateMode.PERSISTENT);
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
	public void dir(String path,String value,CreateMode createMode)throws KeeperException, InterruptedException, IOException{
		byte[] data = value != null ? value.getBytes() : null;
		if(zooKeeper.exists(path, null)==null){
			zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
		}else{
			zooKeeper.setData(path, data, -1);
		}
	}
	
	/**
	 * 获取目录数据
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String dirData(String path) throws KeeperException, InterruptedException{
		byte[] data = zooKeeper.getData(path, null, null);
		if(data == null){
			return null;
		}
		return new String(data);
	}
	
	
	/**
	 * 判断是否存在
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean exists(String path) throws KeeperException, InterruptedException{
		return zooKeeper.exists(path, null)!=null;
	}
	
	public void delete(String path) throws InterruptedException, KeeperException{
		zooKeeper.delete(path, -1);
	}
	
	public void onChildrenChange(String path,Watcher watcher) throws KeeperException, InterruptedException{
		zooKeeper.getChildren(path, new PermanentWatcher(zooKeeper,path,watcher));
	}
	
	public List<String> getChildren(String path) throws KeeperException, InterruptedException{
		return zooKeeper.getChildren(path,null);	
	}
	
	public static class PermanentWatcher implements Watcher{
		ZooKeeper zk;
		String path;
		Watcher watcher;
		public PermanentWatcher(ZooKeeper _zooKeeper,String path,Watcher watcher){
			this.zk = _zooKeeper;
			this.path = path;
			this.watcher = watcher;
		}
		@Override
		public void process(WatchedEvent event) {
			try {
				watcher.process(event);
				zk.getChildren(path, new PermanentWatcher(zk,path,watcher));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
