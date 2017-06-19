/*
 * SonarQube
 * Copyright (C) 2009-2017 SonarSource SA
 * mailto:info AT sonarsource DOT com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.sonar.server.es.queue;

import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.sonar.api.Startable;
import org.sonar.api.config.Settings;
import org.sonar.api.utils.System2;
import org.sonar.api.utils.log.Logger;
import org.sonar.api.utils.log.Loggers;
import org.sonar.core.util.stream.MoreCollectors;
import org.sonar.db.DbClient;
import org.sonar.db.DbSession;
import org.sonar.db.es.EsQueueDto;
import org.sonar.server.user.index.UserIndexer;

import static com.google.common.base.Preconditions.checkState;

public class RecoveryIndexer implements Startable {

  private static final Logger LOGGER = Loggers.get(RecoveryIndexer.class);
  private static final String PROPERTY_DELAY = "sonar.search.recovery.delayInSeconds";
  private static final String PROPERTY_MIN_AGE = "sonar.search.recovery.minAgeInSeconds";
  private static final String PROPERTY_MAX_AGE = "sonar.search.recovery.maxAgeInSeconds";
  private static final String PROPERTY_MAX_RECOVERY_DURATION = "sonar.search.recovery.maxRecoveryDurationInSeconds";
  private static final long DEFAULT_DELAY_IN_SECONDS = 5L * 60;
  private static final long DEFAULT_MIN_AGE_IN_SECONDS = 5L * 60;
  private static final long DEFAULT_MAX_AGE_IN_SECONDS = 60L * 60;
  private static final long DEFAULT_MAX_RECOVERY_DURATION_IN_SECONDS = 2L * 60;

  private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder()
      .setPriority(Thread.MIN_PRIORITY)
      .setNameFormat("RecoveryIndexer-%d")
      .build());
  private final System2 system2;
  private final Settings settings;
  private final DbClient dbClient;
  private final UserIndexer userIndexer;

  // initialized at startup
  private long minAgeInSeconds;
  private long maxAgeInSeconds;
  private long maxRecoveryDurationInSeconds;

  public RecoveryIndexer(System2 system2, Settings settings, DbClient dbClient, UserIndexer userIndexer) {
    this.system2 = system2;
    this.settings = settings;
    this.dbClient = dbClient;
    this.userIndexer = userIndexer;
  }

  @Override
  public void start() {
    long delayInSeconds = getSetting(PROPERTY_DELAY, DEFAULT_DELAY_IN_SECONDS);
    minAgeInSeconds = getSetting(PROPERTY_MIN_AGE, DEFAULT_MIN_AGE_IN_SECONDS);
    maxAgeInSeconds = getSetting(PROPERTY_MAX_AGE, DEFAULT_MAX_AGE_IN_SECONDS);
    maxRecoveryDurationInSeconds = getSetting(PROPERTY_MAX_RECOVERY_DURATION, DEFAULT_MAX_RECOVERY_DURATION_IN_SECONDS);
    checkState(minAgeInSeconds < maxAgeInSeconds,
      "Parameter [%s: %s] must be lower than [%s: %s]", PROPERTY_MAX_AGE, minAgeInSeconds, PROPERTY_MAX_AGE, maxAgeInSeconds);

    executorService.scheduleAtFixedRate(
      this::index,
      0,
      delayInSeconds,
      TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    executorService.shutdown();
  }

  protected void index() {
    long startedAt = system2.now();
    long beforeDate = startedAt - minAgeInSeconds * 1_000;
    long afterDate = startedAt - maxAgeInSeconds * 1_000;

    try (DbSession dbSession = dbClient.openSession(false)) {
      Collection<EsQueueDto> items = dbClient.esQueueDao().selectForRecovery(dbSession, beforeDate, afterDate);
      while (!items.isEmpty()) {
        if (system2.now() > startedAt + maxRecoveryDurationInSeconds * 1_000) {
          LOGGER.warn("Elasticsearch recovery - timeout for recovery reached. Documents will be indexed at next run (see property {})", PROPERTY_DELAY);
          break;
        }
        ListMultimap<EsQueueDto.Type, EsQueueDto> itemsByType = items.stream()
          .collect(MoreCollectors.index(EsQueueDto::getDocType));
        itemsByType.asMap().forEach((type, typeItems) -> {
          LOGGER.trace("Elasticsearch recovery - processing {} {}", typeItems.size(), type);
          switch (type) {
            case USER:
              userIndexer.index(dbSession, typeItems);
              break;
            default:
              LOGGER.error("Elasticsearch recovery - ignore {} documents with unsupported type {}", typeItems.size(), type);
              break;
          }
        });
        items = dbClient.esQueueDao().selectForRecovery(dbSession, beforeDate, afterDate);
      }
    } catch (Throwable t) {
      LOGGER.error("Elasticsearch recovery - fail to recover documents", t);
    }
  }

  private long getSetting(String key, long defaultValue) {
    long val = settings.getLong(key);
    if (val <= 0) {
      val = defaultValue;
    }
    LOGGER.debug("Elasticsearch recovery - {}={}", key, val);
    return val;
  }
}
