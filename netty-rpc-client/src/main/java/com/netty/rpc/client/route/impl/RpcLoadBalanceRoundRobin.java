package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.protocol.ServerConfigInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceRoundRobin extends RpcLoadBalance {
    private AtomicInteger roundRobin = new AtomicInteger(0);

    public ServerConfigInfo doRoute(List<ServerConfigInfo> addressList) {
        int size = addressList.size();
        // Round robin
        int index = (roundRobin.getAndAdd(1) + size) % size;
        return addressList.get(index);
    }

    @Override
    public ServerConfigInfo route(String serviceKey, Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<ServerConfigInfo>> serviceMap = getServiceMap(connectedServerNodes);
        List<ServerConfigInfo> addressList = serviceMap.get(serviceKey);
        if (addressList != null && addressList.size() > 0) {
            return doRoute(addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
