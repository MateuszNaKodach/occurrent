/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Common operations for MongoDB lease-based competing consumer strategies
 */
public class MongoLeaseCompetingConsumerStrategySupport {
    private static final Logger log = LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategySupport.class);

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    private final Clock clock;
    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final Map<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;

    private volatile boolean running;


    public MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, Clock clock, RetryStrategy retryStrategy) {
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.scheduledRefresh = ScheduledRefresh.auto();
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();

        if (retryStrategy instanceof Retry retry) {
            this.retryStrategy = retry.retryIf(retry.retryPredicate.and(__ -> running));
        } else {
            this.retryStrategy = retryStrategy;
        }
    }

    public MongoLeaseCompetingConsumerStrategySupport scheduleRefresh(Function<Consumer<MongoCollection<BsonDocument>>, Runnable> fn) {
        scheduledRefresh.scheduleInBackground(() -> retryStrategy.execute(() -> fn.apply(this::refreshOrAcquireLease).run()), leaseTime);
        return this;
    }

    public boolean registerCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Status oldStatus = competingConsumers.get(competingConsumer);
        boolean acquired = MongoListenerLockService.acquireOrRefreshFor(collection, clock, retryStrategy, leaseTime, subscriptionId, subscriberId).isPresent();
        logDebug("oldStatus={} acquired lock={} (subscriberId={}, subscriptionId={})", oldStatus, acquired, subscriberId, subscriptionId);
        competingConsumers.put(competingConsumer, acquired ? Status.LOCK_ACQUIRED : Status.LOCK_NOT_ACQUIRED);
        if (oldStatus != Status.LOCK_ACQUIRED && acquired) {
            logDebug("Consumption granted (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        } else if (oldStatus == Status.LOCK_ACQUIRED && !acquired) {
            logDebug("Consumption prohibited (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
        return acquired;
    }

    public void unregisterCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Unregistering consumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        Status status = competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
        MongoListenerLockService.remove(collection, retryStrategy, subscriptionId);
        if (status == Status.LOCK_ACQUIRED) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
    }

    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        boolean hasLock = status == Status.LOCK_ACQUIRED;
        logDebug("hasLock={} (subscriberId={}, subscriptionId={})", hasLock, subscriberId, subscriptionId);
        return hasLock;
    }

    public void addListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.add(listenerConsumer);
    }

    public void removeListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.remove(listenerConsumer);
    }

    public void shutdown() {
        logDebug("Shutting down");
        running = false;
        scheduledRefresh.close();
    }

    private void refreshOrAcquireLease(MongoCollection<BsonDocument> collection) {
        logDebug("In refreshOrAcquireLease with {} competing consumers", competingConsumers.size());
        competingConsumers.forEach((cc, status) -> {
            logDebug("Status {} (subscriberId={}, subscriptionId={})", status, cc.subscriberId, cc.subscriptionId);
            if (status == Status.LOCK_ACQUIRED) {
                boolean stillHasLock = MongoListenerLockService.commit(collection, clock, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId);
                if (!stillHasLock) {
                    logDebug("Lost lock! (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                    // Lock was lost!
                    competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                    competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId));
                    logDebug("Completed calling onConsumeProhibited for all listeners (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                }
            } else {
                registerCompetingConsumer(collection, cc.subscriptionId, cc.subscriberId);
            }
        });
    }

    private record CompetingConsumer(String subscriptionId, String subscriberId) {
    }

    private enum Status {
        LOCK_ACQUIRED, LOCK_NOT_ACQUIRED
    }

    private static void logDebug(String message, Object... params) {
        log.debug(message, params);
    }
}