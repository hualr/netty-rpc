package com.netty.rpc.client.discovery;

import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.config.Constant;
import com.netty.rpc.protocol.ServerConfigInfo;
import com.netty.rpc.zookeeper.CuratorClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务发现
 *
 * @author luxiaoxun
 */
public class ServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);
    private final CuratorClient curatorClient;

    public ServiceDiscovery(String registryAddress) {
        this.curatorClient = new CuratorClient(registryAddress);
        discoveryService();
    }

    private void discoveryService() {
        try {
            // Get initial service info
            logger.info("Get initial service info");
            getServiceAndUpdateServer();
            // Add watch listener
            curatorClient.watchPathChildrenNode(
                    Constant.ZK_REGISTRY_PATH, (curatorFramework, pathChildrenCacheEvent) -> {
                        PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                        ChildData childData = pathChildrenCacheEvent.getData();
                        switch (type) {
                            case CONNECTION_RECONNECTED:
                                logger.info("Reconnected to zk, try to get latest service list");
                                getServiceAndUpdateServer();
                                break;
                            case CHILD_UPDATED:
                            case CHILD_ADDED:
                            case CHILD_REMOVED:
                                getServiceAndUpdateServer(childData, type);
                                break;
                            default:
                        }
                    });
        } catch (Exception ex) {
            logger.error("Watch node exception: ", ex);
        }
    }

    private void getServiceAndUpdateServer() {
        try {
            List<String> nodeList = curatorClient.getChildren(Constant.ZK_REGISTRY_PATH);
            List<ServerConfigInfo> dataList = new ArrayList<>();
            for (String node : nodeList) {
                logger.debug("Service node: [{}]", node);
                byte[] bytes = curatorClient.getData(Constant.ZK_REGISTRY_PATH + "/" + node);
                ServerConfigInfo serverConfigInfo = ServerConfigInfo.fromJson(new String(bytes));
                dataList.add(serverConfigInfo);
            }
            logger.debug("Service node data: {}", dataList);
            //Update the service info based on the latest data
            UpdateConnectedServer(dataList);
        } catch (Exception e) {
            logger.error("Get node exception: ", e);
        }
    }

    private void getServiceAndUpdateServer(ChildData childData, PathChildrenCacheEvent.Type type) {
        String path = childData.getPath();
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        logger.info("Child data updated, path:{},type:{},data:{},", path, type, data);
        ServerConfigInfo serverConfigInfo = ServerConfigInfo.fromJson(data);
        updateConnectedServer(serverConfigInfo, type);
    }

    private void UpdateConnectedServer(List<ServerConfigInfo> dataList) {
        ConnectionManager.getInstance().updateConnectedServer(dataList);
    }


    private void updateConnectedServer(ServerConfigInfo serverConfigInfo, PathChildrenCacheEvent.Type type) {
        ConnectionManager.getInstance().updateConnectedServer(serverConfigInfo, type);
    }

    public void stop() {
        this.curatorClient.close();
    }
}
