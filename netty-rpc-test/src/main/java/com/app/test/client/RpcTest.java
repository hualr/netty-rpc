package com.app.test.client;

import com.app.test.service.HelloService;
import com.netty.rpc.client.RpcClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by luxiaoxun on 2016-03-11.
 */
public class RpcTest {

    public static void main(String[] args) throws InterruptedException {
        final RpcClient rpcClient = new RpcClient("101.201.67.114:2181");
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        int threadNum = 1;
        final int requestNum = 50;
        long startTime = System.currentTimeMillis();
        //benchmark for sync call
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            Runnable runnable = () -> {
                for (int i1 = 0; i1 < requestNum; i1++) {
                    try {
                        final HelloService syncClient = rpcClient.createService(HelloService.class, "1.0");
                        String result = syncClient.hello(Integer.toString(i1));
                        if (!result.equals("Hello " + i1)) {
                            System.out.println("error = " + result);
                        } else {
                            System.out.println("result = " + result);
                        }
                        try {
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception ex) {
                        System.out.println(ex.toString());
                    }
                }
                countDownLatch.countDown();
            };
            executorService.execute(runnable);
        }

        countDownLatch.await();
        long timeCost = (System.currentTimeMillis() - startTime);
        String msg = String.format("Sync call total-time-cost:%sms, req/s=%s", timeCost,
                ((double) (requestNum * threadNum)) / timeCost * 1000);
        System.out.println(msg);
        rpcClient.stop();
    }
}
