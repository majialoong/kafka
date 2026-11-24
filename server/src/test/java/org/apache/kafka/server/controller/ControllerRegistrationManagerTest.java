/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.controller;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.message.ControllerRegistrationResponseData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.ControllerRegistrationResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.internals.ExponentialBackoff;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.metadata.ListenerInfo;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.ControllerInformation;
import org.apache.kafka.server.MockNodeToControllerChannelManager;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLER_REGISTRATION;
import static org.apache.kafka.metadata.RecordTestUtils.createTestControllerRegistration;
import static org.apache.kafka.metadata.RecordTestUtils.createTestControllerUnregistration;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_6_IV0;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_7_IV0;
import static org.apache.kafka.server.common.MetadataVersion.IBP_4_4_IV2;
import static org.apache.kafka.server.common.MetadataVersion.IBP_4_5_IV0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
public class ControllerRegistrationManagerTest {
    private static final Node CONTROLLER = new Node(1, "localhost", 7000);
    private static final ControllerInformation CONTROLLER_INFO = new ControllerInformation(
        Optional.of(CONTROLLER), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
        SecurityProtocol.PLAINTEXT, SaslConfigs.DEFAULT_SASL_MECHANISM);
    private static final MetadataProvenance PROVENANCE = new MetadataProvenance(100, 200, 300, true);
    private static final SnapshotManifest SNAPSHOT = new SnapshotManifest(PROVENANCE, 1000);
    private static final LogDeltaManifest LOG_DELTA = LogDeltaManifest.newBuilder()
        .provenance(PROVENANCE)
        .leaderAndEpoch(new LeaderAndEpoch(OptionalInt.of(CONTROLLER.id()), 100))
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(200)
        .build();

    private final MockTime time = new MockTime(1, 1);
    private final MockClient client = new MockClient(time);
    private final MockNodeToControllerChannelManager channelManager = new MockNodeToControllerChannelManager(
        client, time, () -> CONTROLLER_INFO, NodeApiVersions.create(CONTROLLER_REGISTRATION.id,
            CONTROLLER_REGISTRATION.oldestVersion(), CONTROLLER_REGISTRATION.latestVersion()));
    private final ControllerRegistrationManager manager = new ControllerRegistrationManager(
        CONTROLLER.id(), time, "controller-registration-manager-test-",
        Map.of(MetadataVersion.FEATURE_NAME,
            VersionRange.of(MetadataVersion.MINIMUM_VERSION.featureLevel(), IBP_3_7_IV0.featureLevel())),
        registration(CONTROLLER.id()).incarnationId(),
        ListenerInfo.create(List.of(new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "localhost", 8001))),
        // Disable jitter so the test can observe a failure before advancing the clock for the retry.
        new ExponentialBackoff(1, 2, 100, 0));

    @AfterEach
    public void tearDown() throws Exception {
        manager.close();
        client.close();
    }

    private static RegisterControllerRecord registration(int id) {
        return createTestControllerRegistration(id, false);
    }

    private MetadataImage publishMetadata(MetadataImage previousImage, MetadataVersion version, ApiMessage... records) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(previousImage).build();
        boolean versionChanged = !previousImage.features().metadataVersion().equals(Optional.of(version));
        if (versionChanged) {
            delta.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(version.featureLevel()));
        }
        for (ApiMessage record : records) {
            delta.replay(record);
        }
        MetadataImage image = delta.apply(PROVENANCE);
        manager.onMetadataUpdate(delta, image, versionChanged ? SNAPSHOT : LOG_DELTA);
        return image;
    }

    private MetadataImage publishOtherControllerRegistrations(MetadataVersion version) {
        return publishMetadata(MetadataImage.EMPTY, version, registration(2), registration(3));
    }

    private record RpcStats(boolean pending, long successful, long failed) { }

    private <T> T onEventQueue(Callable<T> action) throws Exception {
        FutureTask<T> task = new FutureTask<>(action);
        manager.eventQueue().append(task::run);
        return task.get(30, TimeUnit.SECONDS);
    }

    private boolean registeredInLog() throws Exception {
        return onEventQueue(manager::registeredInLog);
    }

    private void assertRpcStats(boolean pending, long successful, long failed) throws Exception {
        assertEquals(new RpcStats(pending, successful, failed),
            onEventQueue(() -> new RpcStats(manager.pendingRpc(), manager.successfulRpcs(), manager.failedRpcs())));
    }

    private void respond(ControllerRegistrationResponseData data) {
        client.prepareResponseFrom(new ControllerRegistrationResponse(data), CONTROLLER);
        channelManager.poll();
    }

    @Test
    public void testCreateAndClose() throws Exception {
        assertFalse(registeredInLog());
        assertRpcStats(false, 0, 0);
    }

    @Test
    public void testCreateStartAndClose() throws Exception {
        manager.start(channelManager);
        assertFalse(registeredInLog());
        assertRpcStats(false, 0, 0);
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_3_6_IV0", "IBP_3_7_IV0"})
    public void testRegistration(MetadataVersion version) throws Exception {
        manager.start(channelManager);
        assertFalse(registeredInLog());
        assertRpcStats(false, 0, 0);

        if (version == IBP_3_6_IV0) {
            publishMetadata(MetadataImage.EMPTY, version);
            assertFalse(registeredInLog());
            assertRpcStats(false, 0, 0);
            return;
        }

        MetadataImage image = publishOtherControllerRegistrations(version);
        assertRpcStats(true, 0, 0);
        respond(new ControllerRegistrationResponseData());
        assertRpcStats(false, 1, 0);
        assertFalse(registeredInLog());

        publishMetadata(image, version, registration(CONTROLLER.id()));
        assertTrue(registeredInLog());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_4_4_IV2", "IBP_4_5_IV0"})
    public void testRequestVersionFollowsRequestHeaderVersion(MetadataVersion version) throws Exception {
        var capturedRequest = new AtomicReference<ControllerRegistrationRequest>();
        client.prepareResponseFrom(body -> {
            capturedRequest.set((ControllerRegistrationRequest) body);
            return true;
        }, new ControllerRegistrationResponse(new ControllerRegistrationResponseData()), CONTROLLER);
        manager.start(channelManager);
        publishOtherControllerRegistrations(version);
        // Let the manager process the metadata update before polling the channel.
        onEventQueue(() -> null);
        channelManager.poll();

        ControllerRegistrationRequest request = capturedRequest.get();
        assertNotNull(request, "Expected a controller registration request");
        short requestVersion = request.version();
        short headerVersion = CONTROLLER_REGISTRATION.requestHeaderVersion(
            version.isAtLeast(IBP_4_5_IV0) ? CONTROLLER_REGISTRATION.latestVersion() : CONTROLLER_REGISTRATION.oldestVersion());
        assertEquals(CONTROLLER_REGISTRATION.latestVersionForRequestHeader(headerVersion), requestVersion);
        assertEquals(headerVersion, CONTROLLER_REGISTRATION.requestHeaderVersion(requestVersion));
    }

    @Test
    public void testWrongIncarnationId() throws Exception {
        publishMetadata(MetadataImage.EMPTY, IBP_3_7_IV0,
            registration(CONTROLLER.id()).setIncarnationId(new Uuid(456, CONTROLLER.id())));
        manager.start(channelManager);
        assertRpcStats(true, 0, 0);
        respond(new ControllerRegistrationResponseData());
        assertRpcStats(false, 1, 0);

        publishMetadata(MetadataImage.EMPTY, IBP_3_7_IV0,
            registration(CONTROLLER.id()).setIncarnationId(new Uuid(457, CONTROLLER.id())));
        assertRpcStats(true, 1, 0);
    }

    @Test
    public void testRetransmitRegistration() throws Exception {
        manager.start(channelManager);
        publishOtherControllerRegistrations(IBP_3_7_IV0);
        assertRpcStats(true, 0, 0);

        respond(new ControllerRegistrationResponseData()
            .setErrorCode(Errors.UNKNOWN_CONTROLLER_ID.code()).setErrorMessage("Unknown controller 1"));
        assertRpcStats(false, 0, 1);
        time.sleep(1);
        assertRpcStats(true, 0, 1);

        respond(new ControllerRegistrationResponseData());
        assertRpcStats(false, 1, 0);
    }

    @Test
    public void testRetransmitRegistrationAfterTimeout() throws Exception {
        manager.start(channelManager);
        assertFalse(registeredInLog());
        assertRpcStats(false, 0, 0);
        publishOtherControllerRegistrations(IBP_3_7_IV0);
        assertRpcStats(true, 0, 0);
        assertEquals(1, channelManager.unsentQueue().size());

        time.sleep(channelManager.getTimeoutMs());
        channelManager.poll();
        assertRpcStats(false, 0, 1);
        assertEquals(0, channelManager.unsentQueue().size());

        time.sleep(1);
        assertRpcStats(true, 0, 1);
        assertEquals(1, channelManager.unsentQueue().size());
    }

    @Test
    public void testReRegistrationAfterUnregister() throws Exception {
        manager.start(channelManager);
        MetadataImage image = publishMetadata(MetadataImage.EMPTY, IBP_4_4_IV2,
            registration(CONTROLLER.id()), registration(2), registration(3));
        assertTrue(registeredInLog());

        publishMetadata(image, IBP_4_4_IV2, createTestControllerUnregistration(CONTROLLER.id()));
        assertFalse(registeredInLog());
        assertRpcStats(true, 0, 0);
    }

    @Test
    public void testReRegistrationAfterDifferentIncarnationId() throws Exception {
        manager.start(channelManager);
        MetadataImage image = publishOtherControllerRegistrations(IBP_3_7_IV0);
        assertRpcStats(true, 0, 0);
        respond(new ControllerRegistrationResponseData());
        assertRpcStats(false, 1, 0);
        assertFalse(registeredInLog());

        MetadataImage registered = publishMetadata(image, IBP_3_7_IV0, registration(CONTROLLER.id()));
        assertTrue(registeredInLog());

        publishMetadata(registered, IBP_3_7_IV0,
            registration(CONTROLLER.id()).setIncarnationId(new Uuid(999999L, CONTROLLER.id())));
        assertFalse(registeredInLog());
        assertRpcStats(true, 1, 0);
    }
}
