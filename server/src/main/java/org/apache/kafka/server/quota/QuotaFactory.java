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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Sanitizer;
import org.apache.kafka.metadata.publisher.QuotaConfigChangeListener;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaEntity.ConfigEntity;
import org.apache.kafka.server.quota.ClientQuotaManager.ClientIdEntity;
import org.apache.kafka.server.quota.ClientQuotaManager.UserEntity;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.server.quota.ClientQuotaManager.DEFAULT_USER_CLIENT_ID;
import static org.apache.kafka.server.quota.ClientQuotaManager.DEFAULT_USER_ENTITY;

public class QuotaFactory {

    public static final ReplicaQuota UNBOUNDED_QUOTA = new ReplicaQuota() {
        @Override
        public boolean isThrottled(TopicPartition topicPartition) {
            return false;
        }

        @Override
        public boolean isQuotaExceeded() {
            return false;
        }

        @Override
        public void record(long value) {
            // No-op
        }
    };

    public record QuotaManagers(ClientQuotaManager fetch,
                                ClientQuotaManager produce,
                                ClientRequestQuotaManager request,
                                ControllerMutationQuotaManager controllerMutation,
                                ReplicationQuotaManager leader,
                                ReplicationQuotaManager follower,
                                ReplicationQuotaManager alterLogDirs,
                                Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin) {

        public void shutdown() {
            fetch.shutdown();
            produce.shutdown();
            request.shutdown();
            controllerMutation.shutdown();
            clientQuotaCallbackPlugin.ifPresent(plugin -> Utils.closeQuietly(plugin, "client quota callback plugin"));
        }

        public QuotaConfigChangeListener quotaConfigChangeListener() {
            return () -> {
                fetch.updateQuotaMetricConfigs();
                produce.updateQuotaMetricConfigs();
                request.updateQuotaMetricConfigs();
                controllerMutation.updateQuotaMetricConfigs();
            };
        }

        public Map<String, BiConsumer<ClientQuotaEntity, Optional<Quota>>> clientQuotaUpdaters() {
            return Map.of(
                QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, clientQuotaUpdater(fetch),
                QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, clientQuotaUpdater(produce),
                QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, clientQuotaUpdater(request),
                QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, clientQuotaUpdater(controllerMutation)
            );
        }

        private static BiConsumer<ClientQuotaEntity, Optional<Quota>> clientQuotaUpdater(ClientQuotaManager manager) {
            return (entity, quota) -> {
                Map<String, String> entries = entity.entries();
                // An absent entry means no entity; a null name means the default entity.
                Optional<ConfigEntity> userEntity = Optional.empty();
                if (entries.containsKey(USER)) {
                    String user = entries.get(USER);
                    userEntity = Optional.of(user == null ? DEFAULT_USER_ENTITY : new UserEntity(Sanitizer.sanitize(user)));
                }
                Optional<ConfigEntity> clientEntity = Optional.empty();
                if (entries.containsKey(CLIENT_ID)) {
                    String clientId = entries.get(CLIENT_ID);
                    clientEntity = Optional.of(clientId == null ? DEFAULT_USER_CLIENT_ID : new ClientIdEntity(clientId));
                }
                manager.updateQuota(userEntity, clientEntity, quota);
            };
        }
    }

    public static QuotaManagers instantiate(
        AbstractKafkaConfig cfg,
        Metrics metrics,
        Time time,
        String threadNamePrefix,
        String role
    ) {
        Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin = createClientQuotaCallback(cfg, metrics, role);

        return new QuotaManagers(
            new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.FETCH, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.PRODUCE, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ClientRequestQuotaManager(clientConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ControllerMutationQuotaManager(clientControllerMutationConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.LEADER_REPLICATION, time),
            new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.FOLLOWER_REPLICATION, time),
            new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, QuotaType.ALTER_LOG_DIRS_REPLICATION, time),
            clientQuotaCallbackPlugin
        );
    }

    private static Optional<Plugin<ClientQuotaCallback>> createClientQuotaCallback(
        AbstractKafkaConfig cfg,
        Metrics metrics,
        String role
    ) {
        ClientQuotaCallback clientQuotaCallback = cfg.getConfiguredInstance(
            QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, ClientQuotaCallback.class);
        return clientQuotaCallback == null ? Optional.empty() : Optional.of(Plugin.wrapInstance(
            clientQuotaCallback,
            metrics,
            QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG,
            "role", role
        ));
    }

    private static ClientQuotaManagerConfig clientConfig(AbstractKafkaConfig cfg) {
        return new ClientQuotaManagerConfig(
            cfg.quotaConfig().numQuotaSamples(),
            cfg.quotaConfig().quotaWindowSizeSeconds()
        );
    }

    private static ClientQuotaManagerConfig clientControllerMutationConfig(AbstractKafkaConfig cfg) {
        return new ClientQuotaManagerConfig(
            cfg.quotaConfig().numControllerQuotaSamples(),
            cfg.quotaConfig().controllerQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig replicationConfig(AbstractKafkaConfig cfg) {
        return new ReplicationQuotaManagerConfig(
            cfg.quotaConfig().numReplicationQuotaSamples(),
            cfg.quotaConfig().replicationQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig alterLogDirsReplicationConfig(AbstractKafkaConfig cfg) {
        return new ReplicationQuotaManagerConfig(
            cfg.quotaConfig().numAlterLogDirsReplicationQuotaSamples(),
            cfg.quotaConfig().alterLogDirsReplicationQuotaWindowSizeSeconds()
        );
    }
}
