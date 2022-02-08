package com.app.test.client;

import com.app.test.service.HelloService;
import com.netty.rpc.client.RpcClient;
import com.netty.rpc.client.handler.RpcFuture;
import com.netty.rpc.client.proxy.RpcService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by luxiaoxun on 2016/3/16.
 */
public class RpcAsyncTest {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        final RpcClient rpcClient = new RpcClient("101.201.67.114:2181");


        int threadNum = 1;
        final int requestNum = 100;
        Thread[] threads = new Thread[threadNum];

        long startTime = System.currentTimeMillis();
        //benchmark for async call
        CountDownLatch countDownLatch = new CountDownLatch(threadNum * requestNum);

        for (int i = 0; i < threadNum; ++i) {
            Runnable runnable = () -> {
                for (int i1 = 0; i1 < requestNum; i1++) {
                    try {
                        RpcService client = rpcClient.createAsyncService(HelloService.class, "2.0");
                        RpcFuture helloFuture = client.call("hello", Integer.toString(i1));
                        String result = (String) helloFuture.get(3000, TimeUnit.MILLISECONDS);
                        if (!result.equals("Hi " + i1)) {
                            System.out.println("error = " + result);
                        } else {
                            System.out.println("result = " + result);
                        }
                        try {
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
                countDownLatch.countDown();
            };
            executorService.execute(runnable);
        }
        countDownLatch.await();
        long timeCost = (System.currentTimeMillis() - startTime);
        String msg = String.format("Async call total-time-cost:%sms, req/s=%s", timeCost,
                ((double) (requestNum * threadNum)) / timeCost * 1000);
        System.out.println(msg);

        rpcClient.stop();

    }
}
