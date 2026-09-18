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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.metadata.publisher.QuotaEntity;
import org.apache.kafka.metadata.publisher.QuotaEntity.UserClientQuotaEntity;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.quota.QuotaFactory.QuotaManagers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.server.config.QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class QuotaFactoryTest extends BaseClientQuotaManagerTest {

    private final ClientQuotaManagerConfig config = new ClientQuotaManagerConfig();

    private static QuotaManagers quotaManagers(ClientQuotaManager produce) {
        return new QuotaManagers(
            mock(ClientQuotaManager.class), produce, mock(ClientRequestQuotaManager.class),
            mock(ControllerMutationQuotaManager.class), null, null, null, Optional.empty()
        );
    }

    private static Stream<Arguments> metadataUserClientQuotaEntities() {
        var user = new ClientQuotaManager.UserEntity("user");
        var client = new ClientQuotaManager.ClientIdEntity("client");
        return Stream.of(
            Arguments.of(QuotaEntity.user("user"), user, null),
            Arguments.of(QuotaEntity.user(null), ClientQuotaManager.DEFAULT_USER_ENTITY, null),
            Arguments.of(QuotaEntity.clientId("client"), null, client),
            Arguments.of(QuotaEntity.clientId(null), null, ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
            Arguments.of(QuotaEntity.userClient("user", "client"), user, client),
            Arguments.of(QuotaEntity.userClient(null, null),
                ClientQuotaManager.DEFAULT_USER_ENTITY, ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
            Arguments.of(QuotaEntity.user(""), new ClientQuotaManager.UserEntity(""), null),
            Arguments.of(QuotaEntity.clientId(""), null, new ClientQuotaManager.ClientIdEntity("")),
            Arguments.of(QuotaEntity.userClient("", ""),
                new ClientQuotaManager.UserEntity(""), new ClientQuotaManager.ClientIdEntity("")),
            Arguments.of(QuotaEntity.userClient("user /+*", "client /+*"),
                new ClientQuotaManager.UserEntity("user%20%2F%2B%2A"), new ClientQuotaManager.ClientIdEntity("client /+*")),
            Arguments.of(QuotaEntity.userClient("<default>", "<default>"),
                new ClientQuotaManager.UserEntity("%3Cdefault%3E"), new ClientQuotaManager.ClientIdEntity("<default>"))
        );
    }

    @Test
    public void testUserClientQuotaUpdatersDispatchToManagers() {
        QuotaManagers managers = quotaManagers(mock(ClientQuotaManager.class));
        Map<String, ClientQuotaManager> expectedManagers = Map.of(
            CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, managers.fetch(),
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, managers.produce(),
            REQUEST_PERCENTAGE_OVERRIDE_CONFIG, managers.request(),
            CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, managers.controllerMutation()
        );
        var userClientQuotaUpdaters = managers.userClientQuotaUpdaters();
        assertEquals(expectedManagers.keySet(), userClientQuotaUpdaters.keySet());
        var entity = QuotaEntity.userClient("user", "client");
        var quota = Optional.of(Quota.upperBound(123));
        Optional<ClientQuotaEntity.ConfigEntity> user = Optional.of(new ClientQuotaManager.UserEntity("user"));
        Optional<ClientQuotaEntity.ConfigEntity> client = Optional.of(new ClientQuotaManager.ClientIdEntity("client"));
        expectedManagers.forEach((key, manager) -> {
            userClientQuotaUpdaters.get(key).accept(entity, quota);
            verify(manager).updateQuota(user, client, quota);
            userClientQuotaUpdaters.get(key).accept(entity, Optional.empty());
            verify(manager).updateQuota(user, client, Optional.empty());
        });
        verifyNoMoreInteractions(managers.fetch(), managers.produce(), managers.request(), managers.controllerMutation());
    }

    @ParameterizedTest
    @MethodSource("metadataUserClientQuotaEntities")
    public void testUpdateUserClientQuotaFromMetadataEntity(
        UserClientQuotaEntity entity,
        ClientQuotaEntity.ConfigEntity expectedUser,
        ClientQuotaEntity.ConfigEntity expectedClient
    ) {
        ClientQuotaCallback quotaCallback = mock(ClientQuotaCallback.class);
        ClientQuotaManager manager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "",
            Optional.of(Plugin.wrapInstance(quotaCallback, metrics, "clientQuotaCallback")));
        try {
            var userClientQuotaUpdater = quotaManagers(manager).userClientQuotaUpdaters().get(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG);
            userClientQuotaUpdater.accept(entity, Optional.of(Quota.upperBound(123)));

            ArgumentCaptor<ClientQuotaEntity> entityCaptor = ArgumentCaptor.forClass(ClientQuotaEntity.class);
            verify(quotaCallback).updateQuota(eq(ClientQuotaType.PRODUCE), entityCaptor.capture(), eq(123.0));
            var quotaEntity = assertInstanceOf(ClientQuotaManager.KafkaQuotaEntity.class, entityCaptor.getValue());
            assertEquals(expectedUser, quotaEntity.userEntity());
            assertEquals(expectedClient, quotaEntity.clientIdEntity());
            if (expectedUser == ClientQuotaManager.DEFAULT_USER_ENTITY) {
                assertSame(ClientQuotaManager.DEFAULT_USER_ENTITY, quotaEntity.userEntity());
            }
            if (expectedClient == ClientQuotaManager.DEFAULT_USER_CLIENT_ID) {
                assertSame(ClientQuotaManager.DEFAULT_USER_CLIENT_ID, quotaEntity.clientIdEntity());
            }

            userClientQuotaUpdater.accept(entity, Optional.empty());
            verify(quotaCallback).removeQuota(ClientQuotaType.PRODUCE, quotaEntity);
        } finally {
            manager.shutdown();
        }
    }

    @Test
    public void testMetadataDefaultUserClientQuotaUpdatesExistingMetrics() {
        ClientQuotaManager manager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "");
        try {
            var userClientQuotaUpdater = quotaManagers(manager).userClientQuotaUpdaters().get(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG);
            var entity = QuotaEntity.userClient(null, null);
            userClientQuotaUpdater.accept(entity, Optional.of(Quota.upperBound(2000)));
            assertEquals(2000, manager.quota("user", "client").bound());
            assertTrue(maybeRecord(manager, "user", "client", 2500 * config.numQuotaSamples()) > 0);

            userClientQuotaUpdater.accept(entity, Optional.of(Quota.upperBound(3000)));
            assertEquals(3000, manager.quota("user", "client").bound());
            assertEquals(0, maybeRecord(manager, "user", "client", 0));

            userClientQuotaUpdater.accept(entity, Optional.empty());
            assertFalse(manager.quotasEnabled());
            assertEquals(Long.MAX_VALUE, manager.quota("user", "client").bound());
        } finally {
            manager.shutdown();
        }
    }
}
