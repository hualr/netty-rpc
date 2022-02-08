package com.netty.rpc.client.route.api;

import com.google.common.collect.Maps;
import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.protocol.ServerConfigInfo;
import com.netty.rpc.util.ServiceUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.map.HashedMap;

/**
 * Created by luxiaoxun on 2020-08-01.
 * 获取负载均衡map 这个map的keu为服务的名称(根据服务名称和其版本号确定).values是支持的协议类型
 */
public abstract class RpcLoadBalance {
    // Service map: group by service name
    protected Map<String, List<ServerConfigInfo>> getServiceMap(Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes) {
        if (connectedServerNodes == null || connectedServerNodes.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<String, List<ServerConfigInfo>> serviceMap = new HashedMap<>();
        connectedServerNodes.keySet().forEach(rpcProtocol ->
                rpcProtocol.getServiceInfoList().forEach(serviceInfo -> {
            String serviceKey = ServiceUtil.makeServiceKey(serviceInfo.getServiceName(), serviceInfo.getVersion());
            final List<ServerConfigInfo> serverConfigInfoList = serviceMap.getOrDefault(serviceKey, new ArrayList<>());
            serverConfigInfoList.add(rpcProtocol);
            serviceMap.putIfAbsent(serviceKey, serverConfigInfoList);
        }));
        return serviceMap;
    }

    // Route the connection for service key
    public abstract ServerConfigInfo route(String serviceKey, Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes)
            throws Exception;
}
