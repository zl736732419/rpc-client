package com.zheng.rpc;

import com.zheng.rpc.common.constants.ZkConstants;
import com.zheng.rpc.common.utils.ZkUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.spi.ServiceRegistry;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * zk发现服务器配置
 * Created by zhenglian on 2017/10/17.
 */
public class ServiceDiscovery {
    private Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);
    /**
     * zk集群地址
     */
    private String registryAddress;

    private ZooKeeper zk = null;

    /**
     * 这里是为了确保zk连接建立成功
     */
    private CountDownLatch latch = new CountDownLatch(1);

    /**
     * 存放zk上的服务器节点
     */
    private volatile Map<String, Object> servers = new ConcurrentHashMap<>();


    public ServiceDiscovery(String registryAddress) {
        this.registryAddress = registryAddress;
        // 连接到zk
        connectZkServer();
        if (Optional.ofNullable(zk).isPresent()) {
            watchNode();
        }
    }

    /**
     * 发现新节点
     * @return
     */
    public String discover() {
        String data = null;
        int size = servers.size();
        // 存在新节点，使用即可
        if (size > 0) {
            if (size == 1) {
                data = servers.get(0).toString();
                logger.debug("using only data: {}", data);
            } else {
                data = servers.get(ThreadLocalRandom.current().nextInt(size)).toString();
                logger.debug("using random data: {}", data);
            }
        }
        return data;
    }


    /**
     * 连接到zk服务器
     */
    private void connectZkServer() {
        try {
            zk = new ZooKeeper(registryAddress, ZkConstants.SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    Event.KeeperState state = watchedEvent.getState();
                    if (Objects.equals(state, Event.KeeperState.SyncConnected)) {
                        latch.countDown();
                    }
                }
            });
            latch.await(); // 等待zk连接建立完毕再执行下一步
        } catch (Exception e) {
            logger.error("error: " + e.getLocalizedMessage());
        }

    }

    /**
     * 初始化服务器地址信息
     */
    private void watchNode() {
        Map<String, Object> children = ZkUtil.getChildren(zk, ZkConstants.PARENT_NODE, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 节点改变
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    watchNode();
                }
            }
        });
        servers = children;
    }
}
