/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.InvalidServerTypeException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricType;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.ServerConnection.ConnectionState;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.feature.ApiFeatureContracts;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_IN_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_IN_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_OUT_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_OUT_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_IN_FLIGHT_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUEST_LATENCY_MS_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUEST_LATENCY_MS_MAX;
import static org.apache.fluss.metrics.MetricNames.CLIENT_RESPONSES_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_RESPONSES_RATE_TOTAL;
import static org.apache.fluss.rpc.netty.NettyUtils.getClientSocketChannelClass;
import static org.apache.fluss.rpc.netty.NettyUtils.newEventLoopGroup;
import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link ServerConnection}. */
public class ServerConnectionTest {

    private EventLoopGroup eventLoopGroup;
    private Bootstrap bootstrap;
    private ClientAuthenticator clientAuthenticator;
    private Configuration conf;
    private NettyServer nettyServer;
    private ServerNode serverNode;
    private ServerNode serverNode2;
    private TestingGatewayService service;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        buildNettyServer();

        eventLoopGroup = newEventLoopGroup(1, "fluss-netty-client-test");
        bootstrap =
                new Bootstrap()
                        .group(eventLoopGroup)
                        .channel(getClientSocketChannelClass(eventLoopGroup))
                        .handler(new ClientChannelInitializer(5000, false));
        clientAuthenticator =
                AuthenticationFactory.loadClientAuthenticatorSupplier(new Configuration()).get();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }

        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    @Test
    void testConnectionClose() {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        ConnectionState connectionState = connection.getConnectionState();
        assertThat(connectionState).isEqualTo(ConnectionState.CONNECTING);

        GetTableSchemaRequest request =
                new GetTableSchemaRequest()
                        .setTablePath(
                                new PbTablePath().setDatabaseName("test").setTableName("test"))
                        .setSchemaId(0);
        connection.send(ApiKeys.GET_TABLE_SCHEMA, request);

        CompletableFuture<Void> future = connection.close();
        connectionState = connection.getConnectionState();
        assertThat(connectionState).isEqualTo(ConnectionState.DISCONNECTED);
        assertThat(future.isDone()).isTrue();

        assertThatThrownBy(() -> connection.send(ApiKeys.GET_TABLE_SCHEMA, request).get())
                .rootCause()
                .isInstanceOf(DisconnectException.class)
                .hasMessageContaining("Cannot send request to server");
        future = connection.close();
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testConnectionMetrics() throws ExecutionException, InterruptedException {
        MockMetricRegistry metricRegistry = new MockMetricRegistry();
        ClientMetricGroup client = new ClientMetricGroup(metricRegistry, "client");
        ServerConnection connection =
                new ServerConnection(
                        bootstrap, serverNode, client, clientAuthenticator, (con, ignore) -> {});
        ServerConnection connection2 =
                new ServerConnection(
                        bootstrap, serverNode2, client, clientAuthenticator, (con, ignore) -> {});
        LookupRequest request = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket = request.addBucketsReq();
        pbLookupReqForBucket.setBucketId(1);
        assertThat(metricRegistry.registeredMetrics).hasSize(11);

        connection.send(ApiKeys.LOOKUP, request).get();
        connection2.send(ApiKeys.LOOKUP, request).get();

        assertThat(metricRegistry.registeredMetrics).hasSize(11);
        assertThat(metricRegistry.registeredMetrics.keySet())
                .containsExactlyInAnyOrder(
                        CLIENT_REQUESTS_RATE_AVG,
                        CLIENT_REQUESTS_RATE_TOTAL,
                        CLIENT_RESPONSES_RATE_AVG,
                        CLIENT_RESPONSES_RATE_TOTAL,
                        CLIENT_BYTES_IN_RATE_AVG,
                        CLIENT_BYTES_IN_RATE_TOTAL,
                        CLIENT_BYTES_OUT_RATE_AVG,
                        CLIENT_BYTES_OUT_RATE_TOTAL,
                        CLIENT_REQUEST_LATENCY_MS_AVG,
                        CLIENT_REQUEST_LATENCY_MS_MAX,
                        CLIENT_REQUESTS_IN_FLIGHT_TOTAL);
        Metric metric = metricRegistry.registeredMetrics.get(CLIENT_REQUESTS_RATE_AVG);
        assertThat(metric.getMetricType()).isEqualTo(MetricType.GAUGE);
        assertThat(((Gauge<?>) metric).getValue()).isEqualTo(1.0);
        metric = metricRegistry.registeredMetrics.get(CLIENT_REQUESTS_RATE_TOTAL);
        assertThat(metric.getMetricType()).isEqualTo(MetricType.GAUGE);
        assertThat(((Gauge<?>) metric).getValue()).isEqualTo(2L);
        connection.close().get();
    }

    @Test
    void testEncodingErrorDoesNotInterruptPendingRequests() throws Exception {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        Bootstrap delayedBootstrap =
                new Bootstrap() {
                    @Override
                    public ChannelFuture connect(String host, int port) {
                        return bootstrap
                                .connect(host, port)
                                .addListener(f -> connectionLatch.await(1, TimeUnit.MINUTES));
                    }
                };
        ServerConnection connection =
                new ServerConnection(
                        delayedBootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            OutOfMemoryError error = new OutOfMemoryError("Direct buffer memory");
            ApiMessage request = mock(ApiMessage.class);
            when(request.totalSize()).thenReturn(0).thenThrow(error);
            when(request.writeTo(any(ByteBuf.class))).thenReturn(0);

            CompletableFuture<ApiMessage> failedFuture = connection.send(ApiKeys.LOOKUP, request);
            LookupRequest validRequest = new LookupRequest().setTableId(1);
            validRequest.addBucketsReq().setBucketId(1);
            CompletableFuture<ApiMessage> successfulFuture =
                    connection.send(ApiKeys.LOOKUP, validRequest);
            assertThat(failedFuture).isNotDone();
            assertThat(successfulFuture).isNotDone();

            connectionLatch.countDown();

            assertThatThrownBy(() -> failedFuture.get(20, TimeUnit.SECONDS)).hasCause(error);
            assertThat(successfulFuture.get(20, TimeUnit.SECONDS)).isNotNull();
            assertThat(connection.numInflightRequests()).isZero();
        } finally {
            connectionLatch.countDown();
            connection.close().get();
        }
    }

    @Test
    void testWrongServerType() {
        ServerNode wrongServerTypeNode =
                new ServerNode(
                        serverNode.id(),
                        serverNode.host(),
                        serverNode.port(),
                        ServerType.COORDINATOR);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Bootstrap mockBootstrap =
                new Bootstrap() {
                    @Override
                    public ChannelFuture connect(String host, int port) {
                        return bootstrap
                                .connect(host, port)
                                .addListener(f -> countDownLatch.await(1, TimeUnit.MINUTES));
                    }
                };
        ServerConnection connection =
                new ServerConnection(
                        mockBootstrap,
                        wrongServerTypeNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});

        // Pending request will be rejected with InvalidServerTypeException which is
        // InvalidRequestException.
        CompletableFuture<ApiMessage> future =
                connection.send(ApiKeys.LIST_DATABASES, new ListDatabasesRequest());
        assertThat(future).isNotDone();
        countDownLatch.countDown();
        assertThatThrownBy(future::get)
                .rootCause()
                .isInstanceOf(InvalidServerTypeException.class)
                .hasMessageContaining("Server type mismatch");

        // Later request will be rejected with DisconnectException which is also
        // InvalidRequestException.
        assertThatThrownBy(
                        () ->
                                connection
                                        .send(ApiKeys.LIST_DATABASES, new ListDatabasesRequest())
                                        .get())
                .rootCause()
                .isInstanceOf(DisconnectException.class);
    }

    @Test
    void testRejectBucketCountChangeForOldServer() throws Exception {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            connection.validateVersionCompatibility(
                    ApiKeys.ALTER_TABLE, (short) 0, new AlterTableRequest());

            AlterTableRequest bucketCountRequest = new AlterTableRequest();
            bucketCountRequest.setModifyBucketCount().setNewBucketCount(8);
            assertThatThrownBy(
                            () ->
                                    connection.validateVersionCompatibility(
                                            ApiKeys.ALTER_TABLE, (short) 0, bucketCountRequest))
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("requires ALTER_TABLE version 1 or newer")
                    .hasMessageContaining("negotiated version 0");
        } finally {
            connection.close().get();
        }
    }

    @Test
    void testRejectHistoricalWritesForOldServer() throws Exception {
        nettyServer.close();
        buildNettyServer(new OldWriteGatewayService());

        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            assertThat(connection.send(ApiKeys.PUT_KV, putKvRequest(null)).get())
                    .isInstanceOf(PutKvResponse.class);

            assertThatThrownBy(
                            () ->
                                    connection
                                            .send(ApiKeys.PUT_KV, putKvRequest("dt=20260823"))
                                            .get())
                    .rootCause()
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("require PUT_KV version 3 or newer")
                    .hasMessageContaining("negotiated version 2");

            assertThat(connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null)).get())
                    .isInstanceOf(ProduceLogResponse.class);

            assertThatThrownBy(
                            () ->
                                    connection
                                            .send(
                                                    ApiKeys.PRODUCE_LOG,
                                                    produceLogRequest("dt=20260823"))
                                            .get())
                    .rootCause()
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("require PRODUCE_LOG version 1 or newer")
                    .hasMessageContaining("negotiated version 0");
        } finally {
            connection.close().get();
        }
    }

    @Test
    void testNegotiatedVersionDoesNotWaitForAClosingConnection() throws Exception {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        GetTableSchemaRequest request =
                new GetTableSchemaRequest()
                        .setTablePath(
                                new PbTablePath().setDatabaseName("test").setTableName("test"))
                        .setSchemaId(0);

        // the request is still pending while the connection is being established, so close()
        // completes it. It does so while it holds the connection lock, and a writer
        // callback may then wait for a thread that reads the negotiated versions, as the
        // RecordAccumulator does through its deque lock
        CompletableFuture<Boolean> readWhileClosing = new CompletableFuture<>();
        CompletableFuture<ApiMessage> pending = connection.send(ApiKeys.GET_TABLE_SCHEMA, request);
        pending.whenComplete(
                (response, cause) -> {
                    CountDownLatch read = new CountDownLatch(1);
                    Thread reader =
                            new Thread(
                                    () -> {
                                        connection.negotiatedVersion(ApiKeys.PRODUCE_LOG);
                                        connection.finalizedFeatureLevel(LOG_RECORD_BATCH_VERSION);
                                        read.countDown();
                                    });
                    reader.start();
                    try {
                        readWhileClosing.complete(read.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        readWhileClosing.completeExceptionally(e);
                    }
                });
        connection.close().get();
        assertThat(readWhileClosing.get()).isTrue();
    }

    @Test
    void testRenegotiationPicksUpVersionsAdvertisedAfterARaise() throws Exception {
        nettyServer.close();
        RaisableGatewayService raisable = new RaisableGatewayService();
        buildNettyServer(raisable);

        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            assertThat(connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null)).get())
                    .isInstanceOf(ProduceLogResponse.class);
            // before the raise the server advertises PRODUCE_LOG up to v1 and reports level 2
            assertThat(connection.negotiatedVersion(ApiKeys.PRODUCE_LOG)).isEqualTo((short) 1);
            assertThat(connection.finalizedFeatureLevel(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);

            // after the raise, the connection keeps the versions it negotiated until it asks again
            raisable.raised = true;
            assertThat(connection.negotiatedVersion(ApiKeys.PRODUCE_LOG)).isEqualTo((short) 1);
            connection.renegotiateApiVersions().get();
            assertThat(connection.negotiatedVersion(ApiKeys.PRODUCE_LOG)).isEqualTo((short) 2);
            assertThat(connection.finalizedFeatureLevel(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
            assertThat(connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null)).get())
                    .isInstanceOf(ProduceLogResponse.class);
        } finally {
            connection.close().get();
        }
    }

    @Test
    void testProduceLogVersion2AloneImpliesTheRaisedLevel() throws Exception {
        nettyServer.close();
        RaisableGatewayService raisable = new RaisableGatewayService();
        raisable.raised = true;
        // a server that advertises v2 but reports no finalized features, for example one whose
        // response lost the field, still counts as having finalized level 3
        raisable.reportFeatures = false;
        buildNettyServer(raisable);

        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null)).get();
            assertThat(connection.negotiatedVersion(ApiKeys.PRODUCE_LOG)).isEqualTo((short) 2);
            assertThat(connection.finalizedFeatureLevel(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        } finally {
            connection.close().get();
        }
    }

    private static PutKvRequest putKvRequest(String originalPartitionName) {
        PutKvRequest request = new PutKvRequest().setTableId(1L).setAcks(1).setTimeoutMs(10_000);
        request.addBucketsReq().setBucketId(0).setRecords(new byte[0]);
        if (originalPartitionName != null) {
            request.getBucketsReqAt(0).setOriginalPartitionName(originalPartitionName);
        }
        return request;
    }

    private static ProduceLogRequest produceLogRequest(String originalPartitionName) {
        ProduceLogRequest request =
                new ProduceLogRequest().setTableId(1L).setAcks(1).setTimeoutMs(10_000);
        request.addBucketsReq().setBucketId(0).setRecords(new byte[0]);
        if (originalPartitionName != null) {
            request.getBucketsReqAt(0).setOriginalPartitionName(originalPartitionName);
        }
        return request;
    }

    private void buildNettyServer() throws Exception {
        buildNettyServer(new TestingTabletGatewayService());
    }

    private void buildNettyServer(TestingGatewayService gatewayService) throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort();
                NetUtils.Port availablePort2 = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            1, "localhost", availablePort.getPort(), ServerType.TABLET_SERVER);
            serverNode2 =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.TABLET_SERVER);
            service = gatewayService;
            MetricGroup metricGroup = NOPMetricsGroup.newInstance();
            nettyServer =
                    new NettyServer(
                            conf,
                            Arrays.asList(
                                    new Endpoint(serverNode.host(), serverNode.port(), "INTERNAL"),
                                    new Endpoint(serverNode2.host(), serverNode2.port(), "CLIENT")),
                            service,
                            metricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
            nettyServer.start();
        }
    }

    /** A tablet server that advertises PRODUCE_LOG v2 only once the V3 level was raised. */
    private static class RaisableGatewayService extends TestingTabletGatewayService {
        private volatile boolean raised;
        private volatile boolean reportFeatures = true;

        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            return super.apiVersions(request)
                    .thenApply(
                            response -> {
                                ClusterFeatureLevels levels =
                                        raised
                                                ? ClusterFeatureLevels.latest()
                                                : ClusterFeatureLevels.baseline();
                                for (PbApiVersion apiVersion : response.getApiVersionsList()) {
                                    ApiKeys api = ApiKeys.forId(apiVersion.getApiKey());
                                    apiVersion.setMaxVersion(
                                            Math.min(
                                                    apiVersion.getMaxVersion(),
                                                    ApiFeatureContracts.highestAdvertisedVersion(
                                                            api, levels)));
                                }
                                if (reportFeatures) {
                                    levels.reportIn(response);
                                }
                                return response;
                            });
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            return CompletableFuture.completedFuture(new ProduceLogResponse());
        }
    }

    private static class OldWriteGatewayService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            return super.apiVersions(request)
                    .thenApply(
                            response -> {
                                for (PbApiVersion apiVersion : response.getApiVersionsList()) {
                                    if (apiVersion.getApiKey() == ApiKeys.PUT_KV.id) {
                                        apiVersion.setMaxVersion(2);
                                    } else if (apiVersion.getApiKey() == ApiKeys.PRODUCE_LOG.id) {
                                        apiVersion.setMaxVersion(0);
                                    }
                                }
                                return response;
                            });
        }

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            return CompletableFuture.completedFuture(new PutKvResponse());
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            return CompletableFuture.completedFuture(new ProduceLogResponse());
        }
    }

    private static class MockMetricRegistry extends NOPMetricRegistry {

        Map<String, Metric> registeredMetrics = new HashMap<>();

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            registeredMetrics.put(metricName, metric);
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
            registeredMetrics.remove(metricName, metric);
        }
    }
}
