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

import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;

/**
 * Quota entities with names as stored in metadata.
 */
public sealed interface QuotaEntity {
    /**
     * Parses a quota entity, returning empty if unsupported.
     */
    static Optional<QuotaEntity> fromClientQuotaEntity(ClientQuotaEntity entity) {
        Map<String, String> entries = entity.entries();
        if (entries.containsKey(IP)) {
            return Optional.of(new IpEntity(Optional.ofNullable(entries.get(IP))));
        }

        // Each user and client ID dimension can be explicit, default, or absent.
        // Values may be null for defaults, so key presence distinguishes defaults from absent dimensions.
        boolean hasUser = entries.containsKey(USER);
        boolean hasClientId = entries.containsKey(CLIENT_ID);
        String user = entries.get(USER);
        String clientId = entries.get(CLIENT_ID);
        if (hasUser && hasClientId) {
            return Optional.of(userClient(user, clientId));
        }
        if (hasUser) {
            return Optional.of(user(user));
        }
        if (hasClientId) {
            return Optional.of(clientId(clientId));
        }
        return Optional.empty();
    }

    /**
     * Creates a user-only quota entity. A null name denotes the default user.
     */
    static UserClientQuotaEntity user(String user) {
        return new UserClientQuotaEntity(Optional.of(new EntityName(user)), Optional.empty());
    }

    /**
     * Creates a client-ID-only quota entity. A null name denotes the default client ID.
     */
    static UserClientQuotaEntity clientId(String clientId) {
        return new UserClientQuotaEntity(Optional.empty(), Optional.of(new EntityName(clientId)));
    }

    /**
     * Creates a user + client ID quota entity. A null name in either dimension denotes its default.
     */
    static UserClientQuotaEntity userClient(String user, String clientId) {
        return new UserClientQuotaEntity(Optional.of(new EntityName(user)), Optional.of(new EntityName(clientId)));
    }

    /**
     * A user or client ID name, or null for the default entity.
     */
    record EntityName(String name) {
        public boolean isDefault() {
            return name == null;
        }
    }

    /**
     * An IP quota entity. An empty address denotes the default IP entity.
     */
    record IpEntity(Optional<String> ipAddress) implements QuotaEntity { }

    /**
     * A user and/or client ID quota entity. Each dimension is empty when absent, otherwise
     * holds an {@link EntityName} that may be the default (null name).
     */
    record UserClientQuotaEntity(Optional<EntityName> userEntity, Optional<EntityName> clientIdEntity)
        implements QuotaEntity { }
}
