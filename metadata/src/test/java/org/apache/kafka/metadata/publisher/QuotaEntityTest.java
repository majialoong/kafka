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

import org.apache.kafka.common.quota.ClientQuotaEntity;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuotaEntityTest {
    @ParameterizedTest
    @MethodSource("quotaEntities")
    public void testFromClientQuotaEntity(Map<String, String> entries, QuotaEntity expectedEntity) {
        assertEquals(Optional.of(expectedEntity), QuotaEntity.fromClientQuotaEntity(new ClientQuotaEntity(entries)));
    }

    private static Stream<Arguments> quotaEntities() {
        return Stream.of(
            Arguments.of(Map.of(IP, "192.168.1.1"), new QuotaEntity.IpEntity("192.168.1.1")),
            Arguments.of(Collections.singletonMap(IP, null), new QuotaEntity.DefaultIpEntity()),
            Arguments.of(Map.of(USER, "user"), new QuotaEntity.UserEntity("user")),
            Arguments.of(Collections.singletonMap(USER, null), new QuotaEntity.DefaultUserEntity()),
            Arguments.of(Map.of(CLIENT_ID, "client"), new QuotaEntity.ClientIdEntity("client")),
            Arguments.of(Collections.singletonMap(CLIENT_ID, null), new QuotaEntity.DefaultClientIdEntity()),
            Arguments.of(userClientEntityEntries("user", "client"), new QuotaEntity.ExplicitUserExplicitClientIdEntity("user", "client")),
            Arguments.of(userClientEntityEntries("user", null), new QuotaEntity.ExplicitUserDefaultClientIdEntity("user")),
            Arguments.of(userClientEntityEntries(null, "client"), new QuotaEntity.DefaultUserExplicitClientIdEntity("client")),
            Arguments.of(userClientEntityEntries(null, null), new QuotaEntity.DefaultUserDefaultClientIdEntity())
        );
    }

    private static Map<String, String> userClientEntityEntries(String user, String clientId) {
        Map<String, String> entries = new HashMap<>();
        entries.put(USER, user);
        entries.put(CLIENT_ID, clientId);
        return entries;
    }
}
