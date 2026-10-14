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

package org.apache.kafka.network.metrics;

import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestMetricsTest {
    private static final String REQUEST_NAME = "RequestMetricsTest";
    private static final short VERSION = 1;

    private final RequestMetrics metrics = new RequestMetrics(REQUEST_NAME);
    private final ApiKeys apiKey = mock(ApiKeys.class);
    private final ClientInformation clientInformation = new ClientInformation("test-client", "1.0");

    @AfterEach
    void tearDown() {
        metrics.removeMetrics();
    }

    @Test
    void testDeprecatedRequestRate() throws MalformedObjectNameException {
        when(apiKey.isVersionDeprecated(VERSION)).thenReturn(true);
        assertNull(registeredDeprecatedRequestMetric());

        Meter meter = metrics.deprecatedRequestRate(apiKey, VERSION, clientInformation).orElseThrow();

        assertSame(meter, registeredDeprecatedRequestMetric());
        assertSame(meter, metrics.deprecatedRequestRate(apiKey, VERSION, new ClientInformation("test-client", "1.0")).orElseThrow());
    }

    @Test
    void testNonDeprecatedRequestRate() throws MalformedObjectNameException {
        when(apiKey.isVersionDeprecated(VERSION)).thenReturn(false);

        assertTrue(metrics.deprecatedRequestRate(apiKey, VERSION, clientInformation).isEmpty());
        assertNull(registeredDeprecatedRequestMetric());
    }

    private Metric registeredDeprecatedRequestMetric() throws MalformedObjectNameException {
        ObjectName expectedName = new ObjectName("kafka.network:type=RequestMetrics,name=DeprecatedRequestsPerSec,"
            + "request=" + REQUEST_NAME + ",version=1,clientSoftwareName=test-client,clientSoftwareVersion=1.0");
        for (var entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            if (expectedName.equals(new ObjectName(entry.getKey().getMBeanName()))) {
                return entry.getValue();
            }
        }
        return null;
    }
}
