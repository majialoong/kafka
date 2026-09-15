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
            String ip = entries.get(IP);
            return Optional.of(ip == null ? new DefaultIpEntity() : new IpEntity(ip));
        }

        // Each user and client ID dimension can be explicit, default, or absent.
        // Values may be null for defaults, so key presence distinguishes defaults from absent dimensions.
        boolean hasUser = entries.containsKey(USER);
        boolean hasClientId = entries.containsKey(CLIENT_ID);
        String user = entries.get(USER);
        String clientId = entries.get(CLIENT_ID);
        if (hasUser && hasClientId) {
            if (user == null && clientId == null) {
                return Optional.of(new DefaultUserDefaultClientIdEntity());
            }
            if (user == null) {
                return Optional.of(new DefaultUserExplicitClientIdEntity(clientId));
            }
            if (clientId == null) {
                return Optional.of(new ExplicitUserDefaultClientIdEntity(user));
            }
            return Optional.of(new ExplicitUserExplicitClientIdEntity(user, clientId));
        }
        if (hasUser) {
            return Optional.of(user == null ? new DefaultUserEntity() : new UserEntity(user));
        }
        if (hasClientId) {
            return Optional.of(clientId == null ? new DefaultClientIdEntity() : new ClientIdEntity(clientId));
        }
        return Optional.empty();
    }

    /**
     * A user or client ID name, or null for the default entity.
     */
    record EntityName(String name) {
        public boolean isDefault() {
            return name == null;
        }
    }

    sealed interface UserClientQuotaEntity extends QuotaEntity {
        /**
         * Returns the user entity, or empty if the user dimension is absent.
         */
        default Optional<EntityName> userEntity() {
            return Optional.empty();
        }

        /**
         * Returns the client ID entity, or empty if the client ID dimension is absent.
         */
        default Optional<EntityName> clientIdEntity() {
            return Optional.empty();
        }
    }

    sealed interface IpQuotaEntity extends QuotaEntity {
        /**
         * Returns the address from metadata, or an empty Optional for the default IP entity.
         */
        default Optional<String> ipAddress() {
            return Optional.empty();
        }
    }

    record IpEntity(String ip) implements IpQuotaEntity {
        @Override
        public Optional<String> ipAddress() {
            return Optional.of(ip);
        }
    }

    record DefaultIpEntity() implements IpQuotaEntity { }

    record UserEntity(String user) implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(user));
        }
    }

    record DefaultUserEntity() implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(null));
        }
    }

    record ClientIdEntity(String clientId) implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(clientId));
        }
    }

    record DefaultClientIdEntity() implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(null));
        }
    }

    record ExplicitUserExplicitClientIdEntity(String user, String clientId) implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(user));
        }

        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(clientId));
        }
    }

    record ExplicitUserDefaultClientIdEntity(String user) implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(user));
        }

        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(null));
        }
    }

    record DefaultUserExplicitClientIdEntity(String clientId) implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(null));
        }

        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(clientId));
        }
    }

    record DefaultUserDefaultClientIdEntity() implements UserClientQuotaEntity {
        @Override
        public Optional<EntityName> userEntity() {
            return Optional.of(new EntityName(null));
        }

        @Override
        public Optional<EntityName> clientIdEntity() {
            return Optional.of(new EntityName(null));
        }
    }
}
