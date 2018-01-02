package org.xsnake.cloud.zookeeper;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="zookeeper")
public class ZookeeperConfiguration {

	Long timeout;
	
	String url;

	public String getUrl() {
		return url;
	}

	public Long getTimeout() {
		return timeout;
	}

	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
}
