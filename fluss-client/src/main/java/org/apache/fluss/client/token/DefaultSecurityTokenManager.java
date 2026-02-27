/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.token;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.config.ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF;
import static org.apache.fluss.config.ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Manager for security tokens to access files in fluss client. */
public class DefaultSecurityTokenManager implements SecurityTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSecurityTokenManager.class);

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryBackoffPeriod;

    private final SecurityTokenReceiverRepository securityTokenReceiverRepository;

    private final SecurityTokenProvider securityTokenProvider;

    private final ScheduledExecutorService scheduledExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    private final Map<PhysicalTablePath, ScheduledFuture<?>> tokensUpdateFutures = new HashMap<>();

    public DefaultSecurityTokenManager(
            Configuration configuration, SecurityTokenProvider securityTokenProvider) {
        this.securityTokenProvider = securityTokenProvider;
        this.tokensRenewalTimeRatio =
                configuration.get(FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO);
        this.renewalRetryBackoffPeriod =
                configuration.get(FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF).toMillis();

        this.securityTokenReceiverRepository = new SecurityTokenReceiverRepository();

        this.scheduledExecutor =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-token-renew-scheduler"));
    }

    @Override
    public boolean isStarted(PhysicalTablePath tablePath) {
        synchronized (tokensUpdateFutureLock) {
            return tokensUpdateFutures.get(tablePath) != null;
        }
    }

    @Override
    public void start(PhysicalTablePath tablePath) throws Exception {
        synchronized (tokensUpdateFutureLock) {
            if (tokensUpdateFutures.get(tablePath) != null) {
                return;
            }

            startTokensUpdate(tablePath);
        }
    }

    void startTokensUpdate(PhysicalTablePath tablePath) {
        try {
            LOG.info("Starting tokens update task for table {}", tablePath);
            AtomicReference<ObtainedSecurityToken> tokenContainer = new AtomicReference<>();
            Optional<Long> nextRenewal =
                    obtainSecurityTokensAndGetNextRenewal(tablePath, tokenContainer);

            if (tokenContainer.get() != null) {
                securityTokenReceiverRepository.onNewTokensObtained(tokenContainer.get());
            } else {
                LOG.warn("No tokens obtained for table {} so skipping notifications", tablePath);
            }

            if (nextRenewal.isPresent()) {
                long renewalDelay =
                        calculateRenewalDelay(Clock.systemDefaultZone(), nextRenewal.get());
                synchronized (tokensUpdateFutureLock) {
                    ScheduledFuture<?> tokensUpdateFuture =
                            scheduledExecutor.schedule(
                                    () -> startTokensUpdate(tablePath),
                                    renewalDelay,
                                    TimeUnit.MILLISECONDS);
                    tokensUpdateFutures.put(tablePath, tokensUpdateFuture);
                }
                LOG.info(
                        "Tokens update task for table {} started with {} ms delay",
                        tablePath,
                        renewalDelay);
            } else {
                LOG.warn(
                        "Tokens update task for table {} not started because either no tokens obtained or none of the tokens specified its renewal date",
                        tablePath);
            }
        } catch (Exception e) {
            synchronized (tokensUpdateFutureLock) {
                ScheduledFuture<?> tokensUpdateFuture =
                        scheduledExecutor.schedule(
                                () -> startTokensUpdate(tablePath),
                                renewalRetryBackoffPeriod,
                                TimeUnit.MILLISECONDS);
                tokensUpdateFutures.put(tablePath, tokensUpdateFuture);
            }
            LOG.warn(
                    "Failed to update tokens for table {}, will try again in {} ms",
                    tablePath,
                    renewalRetryBackoffPeriod,
                    e);
        }
    }

    protected Optional<Long> obtainSecurityTokensAndGetNextRenewal(
            PhysicalTablePath tablePath, AtomicReference<ObtainedSecurityToken> tokenContainer) {
        try {
            LOG.debug("Obtaining security token for table {}.", tablePath);
            ObtainedSecurityToken token = securityTokenProvider.obtainSecurityToken(tablePath);
            tokenContainer.set(token);
            checkNotNull(token, "Obtained security tokens must not be null");
            LOG.debug("Obtained security token for table {} successfully", tablePath);
            return token.getValidUntil();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            LOG.error("Failed to obtain security token for table {}.", tablePath, t);
            throw new FlussRuntimeException(t);
        }
    }

    @VisibleForTesting
    void stopTokensUpdate() {

    }

    @VisibleForTesting
    long calculateRenewalDelay(Clock clock, long nextRenewal) {
        long now = clock.millis();
        long renewalDelay = Math.round(tokensRenewalTimeRatio * (nextRenewal - now));
        LOG.debug(
                "Calculated delay on renewal is {}, based on next renewal {} and the ratio {}, and current time {}",
                renewalDelay,
                nextRenewal,
                tokensRenewalTimeRatio,
                now);
        return renewalDelay;
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping security token renewal");

        stopTokensUpdate();

        scheduledExecutor.shutdownNow();

        LOG.info("Stopped security token renewal");
    }
}
