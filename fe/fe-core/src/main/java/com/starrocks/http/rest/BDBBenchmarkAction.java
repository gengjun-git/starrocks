// This file is made available under Elastic License 2.0.

package com.starrocks.http.rest;

import com.starrocks.catalog.Catalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        List<Long> latencyList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                try {
                    while (System.currentTimeMillis() - startTime < timeout) {
                        long startT = System.currentTimeMillis();
                        Catalog.getCurrentCatalog().getEditLog().logBenchmark(data);
                        latencyList.add(System.currentTimeMillis() - startT);
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

        Collections.sort(latencyList);
        long sum = latencyList.stream().mapToLong(Long::longValue).sum();

        response.getContent().append("write ")
                .append(cnt.get()).append(" logs(size: ").append(size).append(" bytes) in ")
                .append(timeout / 1000L).append(" seconds, concurrency is ").append(concurrency).append("\n")
                .append("qps is ").append(((double) (cnt.get())) / timeout * 1000).append("\n")
                .append("mean latency is ").append(((double) sum) / cnt.get()).append(" ms\n")
                .append("50% latency is ").append(latencyList.get((int) (cnt.get() * 0.5))).append(" ms\n")
                .append("66% latency is ").append(latencyList.get((int) (cnt.get() * 0.66))).append(" ms\n")
                .append("75% latency is ").append(latencyList.get((int) (cnt.get() * 0.75))).append(" ms\n")
                .append("80% latency is ").append(latencyList.get((int) (cnt.get() * 0.8))).append(" ms\n")
                .append("90% latency is ").append(latencyList.get((int) (cnt.get() * 0.9))).append(" ms\n")
                .append("95% latency is ").append(latencyList.get((int) (cnt.get() * 0.95))).append(" ms\n")
                .append("98% latency is ").append(latencyList.get((int) (cnt.get() * 0.98))).append(" ms\n")
                .append("99% latency is ").append(latencyList.get((int) (cnt.get() * 0.98))).append(" ms\n")
                .append("100% latency is ").append(latencyList.get(cnt.get() - 1)).append(" ms\n");
        sendResult(request, response);
    }
}
