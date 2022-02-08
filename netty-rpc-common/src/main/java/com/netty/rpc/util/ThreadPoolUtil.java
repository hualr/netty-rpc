package com.netty.rpc.util;

import java.util.concurrent.*;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor makeServerThreadPool(final String serviceName,
                                                          int corePoolSize,
                                                          int maxPoolSize) {
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "netty-rpc-" + serviceName + "-" + r.hashCode()),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
