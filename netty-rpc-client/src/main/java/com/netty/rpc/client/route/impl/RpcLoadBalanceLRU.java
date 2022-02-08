package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.protocol.ServerConfigInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * LRU load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceLRU extends RpcLoadBalance {
    private final ConcurrentMap<String, LinkedHashMap<ServerConfigInfo, ServerConfigInfo>> jobLRUMap =
            new ConcurrentHashMap<>();
    private long CACHE_VALID_TIME = 0;

    public ServerConfigInfo doRoute(String serviceKey, List<ServerConfigInfo> addressList) {
        // cache clear
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            jobLRUMap.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }

        // init lru
        LinkedHashMap<ServerConfigInfo, ServerConfigInfo> lruHashMap = jobLRUMap.get(serviceKey);
        if (lruHashMap == null) {
            /**
             * LinkedHashMap
             * a、accessOrder：ture=访问顺序排序（get/put时排序）/ACCESS-LAST；false=插入顺序排期/FIFO；
             * b、removeEldestEntry：新增元素时将会调用，返回true时会删除最老元素；
             *      可封装LinkedHashMap并重写该方法，比如定义最大容量，超出是返回true即可实现固定长度的LRU算法；
             */
            lruHashMap = new LinkedHashMap<ServerConfigInfo, ServerConfigInfo>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<ServerConfigInfo, ServerConfigInfo> eldest) {
                    if (super.size() > 1000) {
                        return true;
                    } else {
                        return false;
                    }
                }
            };
            jobLRUMap.putIfAbsent(serviceKey, lruHashMap);
        }

        // put new
        for (ServerConfigInfo address : addressList) {
            if (!lruHashMap.containsKey(address)) {
                lruHashMap.put(address, address);
            }
        }
        // remove old
        List<ServerConfigInfo> delKeys = new ArrayList<>();
        for (ServerConfigInfo existKey : lruHashMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }
        if (delKeys.size() > 0) {
            for (ServerConfigInfo delKey : delKeys) {
                lruHashMap.remove(delKey);
            }
        }

        // load
        ServerConfigInfo eldestKey = lruHashMap.entrySet().iterator().next().getKey();
        ServerConfigInfo eldestValue = lruHashMap.get(eldestKey);
        return eldestValue;
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
