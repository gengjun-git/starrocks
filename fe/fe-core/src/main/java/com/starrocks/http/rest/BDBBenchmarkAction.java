// This file is made available under Elastic License 2.0.

package com.starrocks.http.rest;

import com.starrocks.catalog.Catalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;

public class BDBBenchmarkAction extends RestBaseAction {

    public BDBBenchmarkAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/bdb_benchmark", new BDBBenchmarkAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        long timeout = Long.parseLong(request.getSingleParameter("timeout")) * 1000L;
        int size = Integer.parseInt(request.getSingleParameter("msg_size"));
        byte[] data = new byte[size];
        long startTime = System.currentTimeMillis();
        int cnt = 0;
        while (System.currentTimeMillis() - startTime < timeout) {
            Catalog.getCurrentCatalog().getEditLog().logBenchmark(data);
            cnt++;
        }
        response.getContent().append("write ")
                .append(cnt)
                .append(" logs(size: ").append(size).append(" bytes) in ")
                .append(timeout / 1000L)
                .append(" seconds\n");
        sendResult(request, response);
    }
}
