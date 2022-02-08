package com.netty.rpc.server.registry;

import com.google.common.base.Preconditions;
import com.netty.rpc.config.Constant;
import com.netty.rpc.protocol.RpcServiceInfo;
import com.netty.rpc.protocol.ServerConfigInfo;
import com.netty.rpc.util.ServiceUtil;
import com.netty.rpc.zookeeper.CuratorClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务注册
 *
 * @author luxiaoxun
 */
public class ServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    private final CuratorClient curatorClient;
    private final List<String> pathList = new ArrayList<>();

    public ServiceRegistry(String registryAddress) {
        Preconditions.checkNotNull(registryAddress, "register Address is null");
        this.curatorClient = new CuratorClient(registryAddress, 5000);
    }

    public void registerService(String host, int port, Map<String, Object> serviceMap) {
        // Register service info
        List<RpcServiceInfo> serviceInfoList = serviceMap.keySet().stream()
                .map(key -> key.split(ServiceUtil.SERVICE_CONCAT_TOKEN))
                .filter(serviceInfo -> serviceInfo.length > 0)
                .map(serviceInfo -> {
                    logger.info("Register new service: {} ", serviceInfo.toString());
                    return new RpcServiceInfo()
                            .setServiceName(serviceInfo[0])
                            .setVersion(serviceInfo.length == 2 ? serviceInfo[1] : "");
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        try {
            ServerConfigInfo serverConfigInfo = new ServerConfigInfo().setHost(host)
                    .setPort(port)
                    .setServiceInfoList(serviceInfoList);

            String serviceData = serverConfigInfo.toJson();
            byte[] bytes = serviceData.getBytes();
            String path = Constant.ZK_DATA_PATH + "-" + serverConfigInfo.hashCode();
            path = this.curatorClient.createPathData(path, bytes);
            pathList.add(path);
            logger.info("Register {} new service, host: {}, port: {}", serviceInfoList.size(), host, port);
        } catch (Exception e) {
            logger.error("Register service fail, exception", e);
        }

        curatorClient.addConnectionStateListener(
                (curatorFramework, connectionState) -> {
                    if (connectionState == ConnectionState.RECONNECTED) {
                        logger.info("Connection state: {}, register service after reconnected", connectionState);
                        registerService(host, port, serviceMap);
                    }
                });
    }

    public void unregisterService() {
        logger.info("Unregister all service");
        for (String path : pathList) {
            try {
                this.curatorClient.deletePath(path);
            } catch (Exception ex) {
                logger.error("Delete service path error: ", ex);
            }
        }
        this.curatorClient.close();
    }
}
