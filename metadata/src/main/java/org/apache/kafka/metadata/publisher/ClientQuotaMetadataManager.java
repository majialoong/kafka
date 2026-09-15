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

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.ClientQuotaDelta;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.metadata.publisher.QuotaEntity.IpQuotaEntity;
import org.apache.kafka.metadata.publisher.QuotaEntity.UserClientQuotaEntity;
import org.apache.kafka.server.config.QuotaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Applies quota metadata changes to the quota managers.
 */
public class ClientQuotaMetadataManager implements Consumer<ClientQuotasDelta> {
    private static final Logger log = LoggerFactory.getLogger(ClientQuotaMetadataManager.class);

    private final Map<String, BiConsumer<UserClientQuotaEntity, Optional<Quota>>> userClientQuotaUpdaters;
    private final BiConsumer<Optional<InetAddress>, Optional<Integer>> ipQuotaUpdater;

    public ClientQuotaMetadataManager(
        Map<String, BiConsumer<UserClientQuotaEntity, Optional<Quota>>> userClientQuotaUpdaters,
        BiConsumer<Optional<InetAddress>, Optional<Integer>> ipQuotaUpdater
    ) {
        this.userClientQuotaUpdaters = Map.copyOf(userClientQuotaUpdaters);
        this.ipQuotaUpdater = ipQuotaUpdater;
    }

    @Override
    public void accept(ClientQuotasDelta quotasDelta) {
        quotasDelta.changes().forEach(this::update);
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        QuotaEntity.fromClientQuotaEntity(entity).ifPresentOrElse(quotaEntity -> {
            if (quotaEntity instanceof IpQuotaEntity ipEntity) {
                handleIpQuota(ipEntity, quotaDelta);
            } else if (quotaEntity instanceof UserClientQuotaEntity userClientEntity) {
                quotaDelta.changes().forEach((key, value) -> handleUserClientQuotaChange(userClientEntity, key, value));
            }
        }, () -> log.warn("Ignoring unsupported quota entity {}.", entity));
    }

    private void handleIpQuota(IpQuotaEntity ipEntity, ClientQuotaDelta quotaDelta) {
        // An empty Optional identifies the default IP entity.
        Optional<InetAddress> address = ipEntity.ipAddress().map(ip -> {
            try {
                return InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Unable to resolve address " + ip);
            }
        });

        quotaDelta.changes().forEach((key, value) -> {
            // The connection quota manager only handles connection rate limits for IP quota updates.
            if (!key.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, ipEntity);
                return;
            }
            try {
                ipQuotaUpdater.accept(address, value.isPresent() ? Optional.of((int) value.getAsDouble()) : Optional.empty());
            } catch (Throwable t) {
                log.error("Failed to update IP quota {}", ipEntity, t);
            }
        });
    }

    private void handleUserClientQuotaChange(UserClientQuotaEntity entity, String key, OptionalDouble newValue) {
        BiConsumer<UserClientQuotaEntity, Optional<Quota>> userClientQuotaUpdater = userClientQuotaUpdaters.get(key);
        if (userClientQuotaUpdater == null) {
            log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
            return;
        }

        Optional<Quota> quota = newValue.isPresent() ? Optional.of(Quota.upperBound(newValue.getAsDouble())) : Optional.empty();
        try {
            userClientQuotaUpdater.accept(entity, quota);
        } catch (Throwable t) {
            log.error("Failed to update user-client quota {}", entity, t);
        }
    }
}
