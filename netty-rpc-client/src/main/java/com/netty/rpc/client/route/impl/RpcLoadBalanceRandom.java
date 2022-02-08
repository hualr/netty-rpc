package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.protocol.ServerConfigInfo;

import java.util.*;

/**
 * Random load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceRandom extends RpcLoadBalance {
    private Random random = new Random();

    public ServerConfigInfo doRoute(List<ServerConfigInfo> addressList) {
        int size = addressList.size();
        // Random
        return addressList.get(random.nextInt(size));
    }

    @Override
    public ServerConfigInfo route(String serviceKey, Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<ServerConfigInfo>> serviceMap = getServiceMap(connectedServerNodes);
        List<ServerConfigInfo> addressList = serviceMap.get(serviceKey);
        if (addressList != null && !addressList.isEmpty()) {
            return doRoute(addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
