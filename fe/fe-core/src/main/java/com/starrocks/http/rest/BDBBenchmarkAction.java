// This file is made available under Elastic License 2.0.

package com.starrocks.http.rest;

import com.starrocks.catalog.Catalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BDBBenchmarkAction extends RestBaseAction {

    public BDBBenchmarkAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/bdb_benchmark", new BDBBenchmarkAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        final long timeout = Long.parseLong(request.getSingleParameter("timeout")) * 1000L;
        int size = Integer.parseInt(request.getSingleParameter("msg_size"));
        int concurrency = Integer.parseInt(request.getSingleParameter("concurrency"));
        final byte[] data = new byte[size];
        final long startTime = System.currentTimeMillis();
        AtomicInteger cnt = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                try {
                    while (System.currentTimeMillis() - startTime < timeout) {
                        Catalog.getCurrentCatalog().getEditLog().logBenchmark(data);
                        cnt.getAndIncrement();
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        response.getContent().append("write ")
                .append(cnt.get())
                .append(" logs(size: ").append(size).append(" bytes) in ")
                .append(timeout / 1000L)
                .append(" seconds, concurrency is ")
                .append(concurrency).append("\n");
        sendResult(request, response);
    }
}
