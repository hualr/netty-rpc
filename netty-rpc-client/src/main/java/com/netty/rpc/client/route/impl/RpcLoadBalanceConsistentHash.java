package com.netty.rpc.client.route.impl;

import com.google.common.hash.Hashing;
import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.protocol.ServerConfigInfo;
import java.util.List;
import java.util.Map;

public class RpcLoadBalanceConsistentHash extends RpcLoadBalance {

    public ServerConfigInfo doRoute(String serviceKey, List<ServerConfigInfo> addressList) {
        int index = Hashing.consistentHash(serviceKey.hashCode(), addressList.size());
        return addressList.get(index);
    }

    @Override
    public ServerConfigInfo route(String serviceKey,
                                  Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes)
            throws Exception {
        Map<String, List<ServerConfigInfo>> serviceMap = getServiceMap(connectedServerNodes);
        List<ServerConfigInfo> addressList = serviceMap.get(serviceKey);
        if (addressList != null && !addressList.isEmpty()) {
            return doRoute(serviceKey, addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
