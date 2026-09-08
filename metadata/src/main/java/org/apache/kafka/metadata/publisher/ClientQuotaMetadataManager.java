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
 * Processes quota metadata changes and updates the quota managers through callbacks,
 * keeping the metadata module independent of the server and network implementations.
 */
public class ClientQuotaMetadataManager implements Consumer<ClientQuotasDelta> {
    private static final Logger log = LoggerFactory.getLogger(ClientQuotaMetadataManager.class);

    private final Map<String, BiConsumer<ClientQuotaEntity, Optional<Quota>>> quotaUpdaters;
    private final BiConsumer<Optional<InetAddress>, Optional<Integer>> ipQuotaUpdater;

    public ClientQuotaMetadataManager(
        Map<String, BiConsumer<ClientQuotaEntity, Optional<Quota>>> quotaUpdaters,
        BiConsumer<Optional<InetAddress>, Optional<Integer>> ipQuotaUpdater
    ) {
        this.quotaUpdaters = Map.copyOf(quotaUpdaters);
        this.ipQuotaUpdater = ipQuotaUpdater;
    }

    @Override
    public void accept(ClientQuotasDelta quotasDelta) {
        quotasDelta.changes().forEach(this::update);
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        if (entity.entries().containsKey(ClientQuotaEntity.IP)) {
            handleIpQuota(entity, quotaDelta);
        } else if (entity.entries().containsKey(ClientQuotaEntity.USER) ||
            entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
            quotaDelta.changes().forEach((key, value) -> handleUserClientQuotaChange(entity, key, value));
        } else {
            log.warn("Ignoring unsupported quota entity {}.", entity);
        }
    }

    private void handleIpQuota(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        String ip = entity.entries().get(ClientQuotaEntity.IP);
        Optional<InetAddress> address;
        try {
            address = ip == null ? Optional.empty() : Optional.of(InetAddress.getByName(ip));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Unable to resolve address " + ip);
        }

        quotaDelta.changes().forEach((key, value) -> {
            if (!key.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
            } else {
                try {
                    ipQuotaUpdater.accept(address, value.isPresent() ? Optional.of((int) value.getAsDouble()) : Optional.empty());
                } catch (Throwable t) {
                    log.error("Failed to update IP quota {}", entity, t);
                }
            }
        });
    }

    private void handleUserClientQuotaChange(ClientQuotaEntity entity, String key, OptionalDouble newValue) {
        BiConsumer<ClientQuotaEntity, Optional<Quota>> updater = quotaUpdaters.get(key);
        if (updater == null) {
            log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
            return;
        }

        Optional<Quota> quota = newValue.isPresent() ? Optional.of(Quota.upperBound(newValue.getAsDouble())) : Optional.empty();
        try {
            updater.accept(entity, quota);
        } catch (Throwable t) {
            log.error("Failed to update user-client quota {}", entity, t);
        }
    }
}
