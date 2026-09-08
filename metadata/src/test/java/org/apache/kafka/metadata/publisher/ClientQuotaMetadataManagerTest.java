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

package org.apache.kafka.metadata.publisher;

import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.ClientQuotasImage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.server.config.QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientQuotaMetadataManagerTest {
    private static final List<String> USER_CLIENT_QUOTA_KEYS = List.of(
        CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
        PRODUCER_BYTE_RATE_OVERRIDE_CONFIG,
        REQUEST_PERCENTAGE_OVERRIDE_CONFIG,
        CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG
    );

    private final List<QuotaUpdate> quotaUpdates = new ArrayList<>();
    private final List<IpQuotaUpdate> ipQuotaUpdates = new ArrayList<>();
    private final ClientQuotaMetadataManager manager = new ClientQuotaMetadataManager(
        quotaUpdaters(),
        (address, quota) -> ipQuotaUpdates.add(new IpQuotaUpdate(address, quota))
    );

    @ParameterizedTest
    @ValueSource(strings = {
        CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
        PRODUCER_BYTE_RATE_OVERRIDE_CONFIG,
        REQUEST_PERCENTAGE_OVERRIDE_CONFIG,
        CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG
    })
    public void testUserClientQuotaUpdateAndRemoval(String key) {
        List<ClientQuotaRecord.EntityData> entities = List.of(
            entity(USER, "CN=alice,OU=users"), entity(CLIENT_ID, "client/id"));
        ClientQuotasDelta update = delta(record(entities, key, 123.5, false));
        ClientQuotaEntity expectedEntity = update.changes().keySet().iterator().next();

        manager.accept(update);
        manager.accept(delta(record(entities, key, 0, true)));

        assertEquals(List.of(
            new QuotaUpdate(key, expectedEntity, Optional.of(Quota.upperBound(123.5))),
            new QuotaUpdate(key, expectedEntity, Optional.empty())
        ), quotaUpdates);
        assertSame(expectedEntity, quotaUpdates.get(0).entity());
        assertTrue(ipQuotaUpdates.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {USER, CLIENT_ID})
    public void testDefaultUserClientEntityIsPreserved(String entityType) {
        manager.accept(delta(record(List.of(entity(entityType, null)), REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 10, false)));

        Map<String, String> expectedEntries = new HashMap<>();
        expectedEntries.put(entityType, null);
        assertEquals(List.of(new QuotaUpdate(REQUEST_PERCENTAGE_OVERRIDE_CONFIG,
            new ClientQuotaEntity(expectedEntries), Optional.of(Quota.upperBound(10)))), quotaUpdates);
        assertTrue(ipQuotaUpdates.isEmpty());
    }

    @Test
    public void testMixedDefaultAndExplicitUserClientEntityIsPreserved() {
        manager.accept(delta(record(List.of(entity(USER, null), entity(CLIENT_ID, "client/id")),
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100, false)));

        Map<String, String> expectedEntries = new HashMap<>();
        expectedEntries.put(USER, null);
        expectedEntries.put(CLIENT_ID, "client/id");
        assertEquals(List.of(new QuotaUpdate(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG,
            new ClientQuotaEntity(expectedEntries), Optional.of(Quota.upperBound(100)))), quotaUpdates);
        assertTrue(ipQuotaUpdates.isEmpty());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"192.168.1.1", "2001:db8::1"})
    public void testIpQuotaUpdateAndRemoval(String address) throws UnknownHostException {
        List<ClientQuotaRecord.EntityData> entities = List.of(entity(IP, address));

        manager.accept(delta(record(entities, IP_CONNECTION_RATE_OVERRIDE_CONFIG, 123.9, false)));
        manager.accept(delta(record(entities, IP_CONNECTION_RATE_OVERRIDE_CONFIG, 0, true)));

        Optional<InetAddress> expectedAddress = address == null ? Optional.empty() : Optional.of(InetAddress.getByName(address));
        assertEquals(List.of(
            new IpQuotaUpdate(expectedAddress, Optional.of(123)),
            new IpQuotaUpdate(expectedAddress, Optional.empty())
        ), ipQuotaUpdates);
        assertTrue(quotaUpdates.isEmpty());
    }

    @Test
    public void testInvalidIpAddress() {
        ClientQuotasDelta delta = delta(record(List.of(entity(IP, "invalid address")),
            IP_CONNECTION_RATE_OVERRIDE_CONFIG, 100, false));

        assertThrows(IllegalArgumentException.class, () -> manager.accept(delta));
        assertTrue(ipQuotaUpdates.isEmpty());
        assertTrue(quotaUpdates.isEmpty());
    }

    @Test
    public void testUnsupportedKeysAndEntitiesAreIgnored() {
        manager.accept(delta(
            record(List.of(entity(USER, "user")), "unknown", 100, false),
            record(List.of(entity(USER, "user")), IP_CONNECTION_RATE_OVERRIDE_CONFIG, 100, false),
            record(List.of(entity(IP, "192.168.1.1")), "unknown", 100, false),
            record(List.of(entity(IP, "192.168.1.1")), PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100, false),
            record(List.of(entity("unknown", "entity")), PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100, false),
            record(List.of(), PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100, false)
        ));

        assertTrue(ipQuotaUpdates.isEmpty());
        assertTrue(quotaUpdates.isEmpty());
    }

    @Test
    public void testIpEntityTakesPrecedence() throws UnknownHostException {
        List<ClientQuotaRecord.EntityData> entities = List.of(entity(IP, "192.168.1.1"), entity(USER, "user"));

        manager.accept(delta(
            record(entities, IP_CONNECTION_RATE_OVERRIDE_CONFIG, 100, false),
            record(entities, PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200, false)
        ));

        assertEquals(List.of(new IpQuotaUpdate(Optional.of(InetAddress.getByName("192.168.1.1")),
            Optional.of(100))), ipQuotaUpdates);
        assertTrue(quotaUpdates.isEmpty());
    }

    @Test
    public void testUserClientCallbackFailureDoesNotStopRemainingUpdates() {
        AtomicInteger attempts = new AtomicInteger();
        BiConsumer<ClientQuotaEntity, Optional<Quota>> updater = (entity, quota) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("First quota update fails");
            }
        };
        ClientQuotaMetadataManager manager = new ClientQuotaMetadataManager(Map.of(
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, updater,
            CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, updater,
            REQUEST_PERCENTAGE_OVERRIDE_CONFIG, updater
        ), (address, quota) -> ipQuotaUpdates.add(new IpQuotaUpdate(address, quota)));
        ClientQuotasDelta delta = delta(
            record(List.of(entity(USER, "user")), PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100, false),
            record(List.of(entity(USER, "user")), CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 200, false),
            record(List.of(entity(USER, "user")), REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 300, false)
        );

        assertDoesNotThrow(() -> manager.accept(delta));
        assertEquals(3, attempts.get());
        assertTrue(ipQuotaUpdates.isEmpty());
    }

    @Test
    public void testIpCallbackFailureDoesNotStopRemainingUpdates() {
        AtomicInteger attempts = new AtomicInteger();
        ClientQuotaMetadataManager manager = new ClientQuotaMetadataManager(quotaUpdaters(), (address, quota) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("First IP quota update fails");
            }
        });
        ClientQuotasDelta delta = delta(
            record(List.of(entity(IP, "192.168.1.1")), IP_CONNECTION_RATE_OVERRIDE_CONFIG, 100, false),
            record(List.of(entity(IP, null)), IP_CONNECTION_RATE_OVERRIDE_CONFIG, 200, false)
        );

        assertDoesNotThrow(() -> manager.accept(delta));
        assertEquals(2, attempts.get());
        assertTrue(quotaUpdates.isEmpty());
    }

    private Map<String, BiConsumer<ClientQuotaEntity, Optional<Quota>>> quotaUpdaters() {
        Map<String, BiConsumer<ClientQuotaEntity, Optional<Quota>>> updaters = new HashMap<>();
        for (String key : USER_CLIENT_QUOTA_KEYS) {
            updaters.put(key, (entity, quota) -> quotaUpdates.add(new QuotaUpdate(key, entity, quota)));
        }
        return updaters;
    }

    private static ClientQuotaRecord.EntityData entity(String type, String name) {
        return new ClientQuotaRecord.EntityData().setEntityType(type).setEntityName(name);
    }

    private static ClientQuotaRecord record(List<ClientQuotaRecord.EntityData> entities, String key, double value, boolean remove) {
        return new ClientQuotaRecord().setEntity(entities).setKey(key).setValue(value).setRemove(remove);
    }

    private static ClientQuotasDelta delta(ClientQuotaRecord... records) {
        ClientQuotasDelta delta = new ClientQuotasDelta(ClientQuotasImage.EMPTY);
        for (ClientQuotaRecord record : records) {
            delta.replay(record);
        }
        return delta;
    }

    private record QuotaUpdate(String key, ClientQuotaEntity entity, Optional<Quota> quota) { }

    private record IpQuotaUpdate(Optional<InetAddress> address, Optional<Integer> quota) { }
}
