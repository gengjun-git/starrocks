// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ThriftServerEventProcessor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ThriftServerEventProcessor implements TServerEventHandler {
    private static final Logger LOG = LogManager.getLogger(ThriftServerEventProcessor.class);

    private ThriftServer thriftServer;

    private static ThreadLocal<ThriftServerContext> connectionContext = new ThreadLocal<>();

    public ThriftServerEventProcessor(ThriftServer thriftServer) {
        this.thriftServer = thriftServer;
    }

    public static ThriftServerContext getConnectionContext() {
        return connectionContext.get();
    }

    @Override
    public void preServe() {
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
        // param input is class org.apache.thrift.protocol.TBinaryProtocol
        SocketAddress socketAddress = null;
        TTransport transport = input.getTransport();

        try {
            switch (thriftServer.getType()) {
                case THREADED:
                    // class org.apache.thrift.transport.TFramedTransport
                    Preconditions.checkState(transport instanceof TFramedTransport);
                    TFramedTransport framedTransport = (TFramedTransport) transport;
                    socketAddress = framedTransport.getClientAddress();
                    break;
                case SIMPLE:
                case THREAD_POOL:
                    // org.apache.thrift.transport.TSocket
                    Preconditions.checkState(transport instanceof TSocket);
                    socketAddress = ((TSocket) transport).getSocket().getRemoteSocketAddress();
                    break;
            }
        } catch (Throwable t) {
            LOG.error("get thrift client socket failed", t);
        }
        if (socketAddress == null) {
            LOG.info("fail to get client address. server type: {}", thriftServer.getType());
            return null;
        }
        InetSocketAddress inetSocketAddress = null;
        if (socketAddress instanceof InetSocketAddress) {
            inetSocketAddress = (InetSocketAddress) socketAddress;
        } else {
            LOG.info("fail to get client socket address. server type: {}",
                    thriftServer.getType());
            return null;
        }
        TNetworkAddress clientAddress = new TNetworkAddress(
                inetSocketAddress.getHostString(),
                inetSocketAddress.getPort());

        thriftServer.addConnect(clientAddress);

        LOG.info("create thrift context. client: {}", clientAddress);
        return new ThriftServerContext(clientAddress);
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
        if (serverContext == null) {
            return;
        }

        Preconditions.checkState(serverContext instanceof ThriftServerContext);
        ThriftServerContext thriftServerContext = (ThriftServerContext) serverContext;
        TNetworkAddress clientAddress = thriftServerContext.getClient();
        connectionContext.remove();
        thriftServer.removeConnect(clientAddress);
        LOG.warn("delete thrift context. client: {}", clientAddress, new Exception());
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
        if (serverContext == null) {
            LOG.warn("serverContext is null");
            return;
        }

        try {
            ThriftServerContext thriftServerContext = (ThriftServerContext) serverContext;
            TNetworkAddress clientAddress = thriftServerContext.getClient();
            Preconditions.checkState(serverContext instanceof ThriftServerContext);
            connectionContext.set(new ThriftServerContext(clientAddress));
        } catch (Throwable t) {
            LOG.warn("process context failed");
        }
    }
}
