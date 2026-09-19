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

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.metadata.publisher.QuotaEntity;
import org.apache.kafka.metadata.publisher.QuotaEntity.UserClientQuotaEntity;
import org.apache.kafka.server.quota.QuotaFactory.QuotaManagers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Stream;

import static org.apache.kafka.server.config.QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class QuotaFactoryTest {
    @ParameterizedTest
    @MethodSource("quotaValues")
    public void testUserClientQuotaUpdaterConvertsEntityAndQuota(OptionalDouble newValue, Optional<Quota> expectedQuota) {
        assertQuotaConversion(
            new QuotaEntity.UserEntity("user"), newValue,
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.empty(), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.DefaultUserEntity(), newValue,
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.empty(), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.ClientIdEntity("client"), newValue,
            Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client")), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.DefaultClientIdEntity(), newValue,
            Optional.empty(), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.ExplicitUserExplicitClientIdEntity("user", "client"), newValue,
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.of(new ClientQuotaManager.ClientIdEntity("client")), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.ExplicitUserDefaultClientIdEntity("user"), newValue,
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.DefaultUserExplicitClientIdEntity("client"), newValue,
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.of(new ClientQuotaManager.ClientIdEntity("client")), expectedQuota
        );
        assertQuotaConversion(
            new QuotaEntity.DefaultUserDefaultClientIdEntity(), newValue,
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), expectedQuota
        );
    }

    private static Stream<Arguments> quotaValues() {
        return Stream.of(
            Arguments.of(OptionalDouble.of(123.5), Optional.of(Quota.upperBound(123.5))),
            Arguments.of(OptionalDouble.empty(), Optional.empty())
        );
    }

    @Test
    public void testUserClientQuotaUpdatersDispatchToManagers() {
        QuotaManagers managers = createQuotaManagers(mock(ClientQuotaManager.class));
        Map<String, ClientQuotaManager> expectedManagers = Map.of(
            CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, managers.fetch(),
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, managers.produce(),
            REQUEST_PERCENTAGE_OVERRIDE_CONFIG, managers.request(),
            CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, managers.controllerMutation()
        );

        var userClientQuotaUpdaters = managers.userClientQuotaUpdaters();
        assertEquals(expectedManagers.keySet(), userClientQuotaUpdaters.keySet());

        var entity = new QuotaEntity.ExplicitUserExplicitClientIdEntity("user", "client");
        var quota = OptionalDouble.of(123.5);
        Optional<ClientQuotaEntity.ConfigEntity> user = Optional.of(new ClientQuotaManager.UserEntity("user"));
        Optional<ClientQuotaEntity.ConfigEntity> client = Optional.of(new ClientQuotaManager.ClientIdEntity("client"));
        expectedManagers.forEach((key, manager) -> {
            userClientQuotaUpdaters.get(key).accept(entity, quota);
            verify(manager).updateQuota(user, client, Optional.of(Quota.upperBound(123.5)));
        });
    }

    private static QuotaManagers createQuotaManagers(ClientQuotaManager produce) {
        return new QuotaManagers(
            mock(ClientQuotaManager.class), produce, mock(ClientRequestQuotaManager.class),
            mock(ControllerMutationQuotaManager.class), null, null, null, Optional.empty()
        );
    }

    private static void assertQuotaConversion(
        UserClientQuotaEntity entity,
        OptionalDouble newValue,
        Optional<ClientQuotaEntity.ConfigEntity> expectedUser,
        Optional<ClientQuotaEntity.ConfigEntity> expectedClient,
        Optional<Quota> expectedQuota
    ) {
        ClientQuotaManager manager = mock(ClientQuotaManager.class);
        var userClientQuotaUpdater = createQuotaManagers(manager).userClientQuotaUpdaters().get(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG);
        userClientQuotaUpdater.accept(entity, newValue);
        verify(manager).updateQuota(expectedUser, expectedClient, expectedQuota);
    }
}
