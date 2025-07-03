// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest;

import com.google.common.collect.Lists;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;

public class ThriftStressTestAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ThriftStressTestAction.class);

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public ThriftStressTestAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/stress", new ThriftStressTestAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String step = request.getSingleParameter("step");
        if ("1".equals(step)) {
            startThreads();
        } else if ("2".equals(step)) {
            countDownLatch.countDown();
        }
        sendResult(request, response);
    }

    private void startThreads() {
        for (int i = 0; i < 9192; i++) {
            Thread t = new Thread(this::call);
            t.setName("stress-test-" + i);
            t.start();
        }
    }

    private void call() {
        try {
            countDownLatch.await();
        } catch (Exception e) {
            LOG.warn("wait failed", e);
        }
        TSetConfigRequest request = new TSetConfigRequest();
        request.setKeys(Lists.newArrayList("max_broker_load_job_concurrency"));
        request.setValues(Lists.newArrayList("1"));
        try {
            TSetConfigResponse response = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.frontendPool,
                    GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderRpcEndpoint(),
                    5000,
                    client -> client.setConfig(request));
            TStatus status = response.getStatus();
            if (status.getStatus_code() != TStatusCode.OK) {
                LOG.warn("set remote fe config failed");
            }
        } catch (Exception e) {
            LOG.warn("set remote fe config failed", e);
        }
    }
}
