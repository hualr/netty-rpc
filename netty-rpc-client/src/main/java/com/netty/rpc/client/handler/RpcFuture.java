package com.netty.rpc.client.handler;

import com.netty.rpc.client.RpcClient;
import com.netty.rpc.codec.RpcRequest;
import com.netty.rpc.codec.RpcResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPCFuture for async RPC call
 * Created by luxiaoxun on 2016-03-15.
 */
public class RpcFuture implements Future<Object> {
    private static final Logger logger = LoggerFactory.getLogger(RpcFuture.class);

    private final Sync sync;
    private final RpcRequest request;
    private RpcResponse response;
    private final long startTime;
    private long responseTimeThreshold = 5000;
    private final List<AsyncRPCCallback> pendingCallbacks = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();

    public RpcFuture(RpcRequest request) {
        this.sync = new Sync();
        this.request = request;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public boolean isDone() {
        return sync.isDone();
    }

    @Override
    public Object get() {
        sync.acquire(1);
        return Optional.ofNullable(this.response).map(RpcResponse::getResult).orElse(null);
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException {
        boolean success = sync.tryAcquireNanos(1, unit.toNanos(timeout));
        if (!success) {
            throw new RuntimeException("Timeout exception. Request id: " + this.request.getRequestId()
                    + ". Request class name: " + this.request.getClassName()
                    + ". Request method: " + this.request.getMethodName());
        }
        return Optional.ofNullable(this.response).map(RpcResponse::getResult).orElse(null);
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    public void done(RpcResponse reponse) {
        this.response = reponse;
        sync.release(1);
        invokeCallbacks();
        // Threshold
        long responseTime = System.currentTimeMillis() - startTime;
        if (responseTime > this.responseTimeThreshold) {
            logger.warn("Service response time is too slow. Request id = " + reponse.getRequestId() + ". Response " +
                    "Time = " + responseTime + "ms");
        }
    }

    private void invokeCallbacks() {
        lock.lock();
        try {
            for (final AsyncRPCCallback callback : pendingCallbacks) {
                runCallback(callback);
            }
        } finally {
            lock.unlock();
        }
    }

    public RpcFuture addCallback(AsyncRPCCallback callback) {
        lock.lock();
        try {
            if (isDone()) {
                runCallback(callback);
            } else {
                this.pendingCallbacks.add(callback);
            }
        } finally {
            lock.unlock();
        }
        return this;
    }

    private void runCallback(final AsyncRPCCallback callback) {
        final RpcResponse res = this.response;
        RpcClient.submit(() -> {
            if (!res.isError()) {
                callback.success(res.getResult());
            } else {
                callback.fail(new RuntimeException("Response error", new Throwable(res.getError())));
            }
        });
    }

    static class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1L;

        //future status
        private static final int DONE = 1;
        private static final int PENDING = 0;

        @Override
        protected boolean tryAcquire(int arg) {
            return getState() == DONE;
        }

        @Override
        protected boolean tryRelease(int arg) {
            if (getState() == PENDING) {
                return compareAndSetState(PENDING, DONE);
            } else {
                return true;
            }
        }

        protected boolean isDone() {
            return getState() == DONE;
        }
    }
}
