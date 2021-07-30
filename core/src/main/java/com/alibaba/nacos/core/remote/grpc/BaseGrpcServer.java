/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.ReflectUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.utils.Loggers;
import io.grpc.Attributes;
import io.grpc.CompressorRegistry;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.DecompressorRegistry;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.internal.ServerStream;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Grpc implementation as a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {

    private Server server;

    private static final String REQUEST_BI_STREAM_SERVICE_NAME = "BiRequestStream";

    private static final String REQUEST_BI_STREAM_METHOD_NAME = "requestBiStream";

    private static final String REQUEST_SERVICE_NAME = "Request";

    private static final String REQUEST_METHOD_NAME = "request";

    private static final String GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY = "nacos.remote.server.grpc.maxinbound.message.size";

    private static final long DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE = 10 * 1024 * 1024;

    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;

    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;

    @Autowired
    private ConnectionManager connectionManager;

    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }

    @Override
    public void startServer() throws Exception {
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();

        // 定义server的拦截器，可以从请求中获取connection id、ip、port等
        // server interceptor to set connection id.
        ServerInterceptor serverInterceptor = new ServerInterceptor() {
            @Override
            public <T, S> ServerCall.Listener<T> interceptCall(ServerCall<T, S> call, Metadata headers,
                    ServerCallHandler<T, S> next) {
                Context ctx = Context.current()
                        .withValue(CONTEXT_KEY_CONN_ID, call.getAttributes().get(TRANS_KEY_CONN_ID))
                        .withValue(CONTEXT_KEY_CONN_REMOTE_IP, call.getAttributes().get(TRANS_KEY_REMOTE_IP))
                        .withValue(CONTEXT_KEY_CONN_REMOTE_PORT, call.getAttributes().get(TRANS_KEY_REMOTE_PORT))
                        .withValue(CONTEXT_KEY_CONN_LOCAL_PORT, call.getAttributes().get(TRANS_KEY_LOCAL_PORT));
                if (REQUEST_BI_STREAM_SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
                    Channel internalChannel = getInternalChannel(call);
                    ctx = ctx.withValue(CONTEXT_KEY_CHANNEL, internalChannel);
                }
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };

        // 添加处理服务
        addServices(handlerRegistry, serverInterceptor);

        // 设置server启动的端口（默认为 8848 + 1001 = 9849），
        // getRpcExecutor线程执行器（线程数默认为 = 处理器核数*16） ，
        // maxInboundMessageSize最大限制为10M，压缩解压缩使用gzip。
        server = ServerBuilder.forPort(getServicePort()).executor(getRpcExecutor())
                .maxInboundMessageSize(getInboundMessageSize()).fallbackHandlerRegistry(handlerRegistry)
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .addTransportFilter(new ServerTransportFilter() {
                    @Override
                    public Attributes transportReady(Attributes transportAttrs) {
                        InetSocketAddress remoteAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                        InetSocketAddress localAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
                        int remotePort = remoteAddress.getPort();
                        int localPort = localAddress.getPort();
                        String remoteIp = remoteAddress.getAddress().getHostAddress();
                        Attributes attrWrapper = transportAttrs.toBuilder()
                                .set(TRANS_KEY_CONN_ID, System.currentTimeMillis() + "_" + remoteIp + "_" + remotePort)
                                .set(TRANS_KEY_REMOTE_IP, remoteIp).set(TRANS_KEY_REMOTE_PORT, remotePort)
                                .set(TRANS_KEY_LOCAL_PORT, localPort).build();
                        String connectionId = attrWrapper.get(TRANS_KEY_CONN_ID);
                        Loggers.REMOTE_DIGEST.info("Connection transportReady,connectionId = {} ", connectionId);
                        return attrWrapper;

                    }

                    @Override
                    public void transportTerminated(Attributes transportAttrs) {
                        String connectionId = null;
                        try {
                            connectionId = transportAttrs.get(TRANS_KEY_CONN_ID);
                        } catch (Exception e) {
                            // Ignore
                        }
                        if (StringUtils.isNotBlank(connectionId)) {
                            Loggers.REMOTE_DIGEST
                                    .info("Connection transportTerminated,connectionId = {} ", connectionId);
                            connectionManager.unregister(connectionId);
                        }
                    }
                }).build();
        // 注册发现server启动（grpc）
        server.start();
    }

    private int getInboundMessageSize() {
        String messageSize = System
                .getProperty(GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY, String.valueOf(DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE));
        return Integer.parseInt(messageSize);
    }

    private Channel getInternalChannel(ServerCall serverCall) {
        ServerStream serverStream = (ServerStream) ReflectUtils.getFieldValue(serverCall, "stream");
        return (Channel) ReflectUtils.getFieldValue(serverStream, "channel");
    }

    private void addServices(MutableHandlerRegistry handlerRegistry, ServerInterceptor... serverInterceptor) {

        // 构造MethodDescriptor，包括：服务调用方式简单RPC即UNARY、服务的接口名和方法名、请求序列化类、响应序列化类
        // unary common call register.
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                // 服务调用方式UNARY
                .setType(MethodDescriptor.MethodType.UNARY)
                // 服务的接口名和方法名「request」
                .setFullMethodName(MethodDescriptor.generateFullMethodName(REQUEST_SERVICE_NAME, REQUEST_METHOD_NAME))
                // 请求序列化类
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                // 响应序列化类
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();

        // 服务接口处理类，接受到request请求将调用执行
        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls
                .asyncUnaryCall((request, responseObserver) -> {
                    grpcCommonRequestAcceptor.request(request, responseObserver);
                });

        // 构建暴露的服务「Request」
        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition.builder(REQUEST_SERVICE_NAME)
                .addMethod(unaryPayloadMethod, payloadHandler).build();

        // 注册到内部的注册中心（Registry）中，可以根据服务定义信息查询实现类（普通对象request/response调用）
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));

        // 服务接口处理类，接收到biRequestStream请求将调用执行
        // bi stream register.
        final ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
                (responseObserver) -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver));

        // 构造MethodDescriptor，包括：服务双向流调用方式BIDI_STREAMING、服务的接口名和方法名、请求序列化类、响应序列化类
        final MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING).setFullMethodName(MethodDescriptor
                        .generateFullMethodName(REQUEST_BI_STREAM_SERVICE_NAME, REQUEST_BI_STREAM_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();

        // 构建暴露的服务「BiRequestStream」
        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition
                .builder(REQUEST_BI_STREAM_SERVICE_NAME).addMethod(biStreamMethod, biStreamHandler).build();

        // 注册到内部的注册中心（Registry）中，可以根据服务定义信息查询实现类（双向流调用）
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));

    }

    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();

    static final Attributes.Key<String> TRANS_KEY_CONN_ID = Attributes.Key.create("conn_id");

    static final Attributes.Key<String> TRANS_KEY_REMOTE_IP = Attributes.Key.create("remote_ip");

    static final Attributes.Key<Integer> TRANS_KEY_REMOTE_PORT = Attributes.Key.create("remote_port");

    static final Attributes.Key<Integer> TRANS_KEY_LOCAL_PORT = Attributes.Key.create("local_port");

    static final Context.Key<String> CONTEXT_KEY_CONN_ID = Context.key("conn_id");

    static final Context.Key<String> CONTEXT_KEY_CONN_REMOTE_IP = Context.key("remote_ip");

    static final Context.Key<Integer> CONTEXT_KEY_CONN_REMOTE_PORT = Context.key("remote_port");

    static final Context.Key<Integer> CONTEXT_KEY_CONN_LOCAL_PORT = Context.key("local_port");

    static final Context.Key<Channel> CONTEXT_KEY_CHANNEL = Context.key("ctx_channel");

}
