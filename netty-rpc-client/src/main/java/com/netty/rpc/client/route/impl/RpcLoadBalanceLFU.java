package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.protocol.ServerConfigInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * LFU load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceLFU extends RpcLoadBalance {
    private final ConcurrentMap<String, HashMap<ServerConfigInfo, Integer>> jobLfuMap = new ConcurrentHashMap<>();
    private long CACHE_VALID_TIME = 0;

    public ServerConfigInfo doRoute(String serviceKey, List<ServerConfigInfo> addressList) {
        // cache clear
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            jobLfuMap.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }

        // lfu item init
        HashMap<ServerConfigInfo, Integer> lfuItemMap = jobLfuMap.get(serviceKey);
        if (lfuItemMap == null) {
            lfuItemMap = new HashMap<ServerConfigInfo, Integer>();
            jobLfuMap.putIfAbsent(serviceKey, lfuItemMap);   // 避免重复覆盖
        }

        // put new
        for (ServerConfigInfo address : addressList) {
            if (!lfuItemMap.containsKey(address) || lfuItemMap.get(address) > 1000000) {
                lfuItemMap.put(address, 0);
            }
        }

        // remove old
        List<ServerConfigInfo> delKeys = new ArrayList<>();
        for (ServerConfigInfo existKey : lfuItemMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }
        if (delKeys.size() > 0) {
            for (ServerConfigInfo delKey : delKeys) {
                lfuItemMap.remove(delKey);
            }
        }

        // load least used count address
        List<Map.Entry<ServerConfigInfo, Integer>> lfuItemList = new ArrayList<Map.Entry<ServerConfigInfo, Integer>>(lfuItemMap.entrySet());
        Collections.sort(lfuItemList, new Comparator<Map.Entry<ServerConfigInfo, Integer>>() {
            @Override
            public int compare(Map.Entry<ServerConfigInfo, Integer> o1, Map.Entry<ServerConfigInfo, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        Map.Entry<ServerConfigInfo, Integer> addressItem = lfuItemList.get(0);
        ServerConfigInfo minAddress = addressItem.getKey();
        addressItem.setValue(addressItem.getValue() + 1);

        return minAddress;
    }

    @Override
    public ServerConfigInfo route(String serviceKey, Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<ServerConfigInfo>> serviceMap = getServiceMap(connectedServerNodes);
        List<ServerConfigInfo> addressList = serviceMap.get(serviceKey);
        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey, addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
