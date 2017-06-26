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
package org.sonar.server.user.index;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.elasticsearch.action.index.IndexRequest;
import org.sonar.db.DbClient;
import org.sonar.db.DbSession;
import org.sonar.db.es.EsQueueDto;
import org.sonar.db.user.UserDto;
import org.sonar.server.es.BulkIndexer;
import org.sonar.server.es.BulkIndexer.Size;
import org.sonar.server.es.EsClient;
import org.sonar.server.es.IndexType;
import org.sonar.server.es.StartupIndexer;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.sonar.db.DatabaseUtils.executeLargeInputsWithoutOutput;
import static org.sonar.server.user.index.UserIndexDefinition.INDEX_TYPE_USER;

public class UserIndexer implements StartupIndexer {

  private final DbClient dbClient;
  private final EsClient esClient;

  public UserIndexer(DbClient dbClient, EsClient esClient) {
    this.dbClient = dbClient;
    this.esClient = esClient;
  }

  @Override
  public Set<IndexType> getIndexTypes() {
    return ImmutableSet.of(INDEX_TYPE_USER);
  }

  @Override
  public void indexOnStartup(Set<IndexType> emptyIndexTypes) {
    doIndex(newBulkIndexer(Size.LARGE), null);
  }

  public void commitAndIndex(DbSession dbSession, UserDto user) {
    EsQueueDto queueItem = dbClient.esQueueDao().insert(dbSession, EsQueueDto.create(EsQueueDto.Type.USER, user.getLogin()));
    dbSession.commit();
    postCommit(dbSession, user, queueItem);
  }

  /**
   * Entry point for Byteman tests. See directory tests/resilience.
   * The unused parameter "user" is used only by the Byteman script.
   */
  private void postCommit(DbSession dbSession, UserDto user, EsQueueDto queueItem) {
    index(dbSession, singletonList(queueItem));
  }

  public void index(DbSession dbSession, Collection<EsQueueDto> esQueueItems) {
    if (esQueueItems.isEmpty()) {
      return;
    }
    esQueueItems.stream()
      .forEach(i -> requireNonNull(i.getUuid(), () -> "BUG - " + i + " has not been persisted before indexing"));

    BulkIndexer bulkIndexer = newBulkIndexer(Size.REGULAR);
    try (UserResultSetIterator rowIt = UserResultSetIterator.create(dbClient, dbSession, esQueueItems)) {
      processResultSet(dbSession, bulkIndexer, esQueueItems, rowIt);
    }
  }

  @Deprecated
  public void index(String login) {
    doIndex(newBulkIndexer(Size.REGULAR), singletonList(login));
  }

  @Deprecated
  public void index(List<String> logins) {
    if (!logins.isEmpty()) {
      doIndex(newBulkIndexer(Size.REGULAR), logins);
    }
  }

  @Deprecated
  public void index(DbSession dbSession, List<String> logins) {
    if (!logins.isEmpty()) {
      doIndex(dbSession, newBulkIndexer(Size.REGULAR), logins);
    }
  }

  @Deprecated
  private void doIndex(BulkIndexer bulk, @Nullable List<String> logins) {
    try (DbSession dbSession = dbClient.openSession(false)) {
      doIndex(dbSession, bulk, logins);
    }
  }

  @Deprecated
  private void doIndex(DbSession dbSession, BulkIndexer bulk, @Nullable List<String> logins) {
    if (logins == null) {
      processLogins(bulk, dbSession, null);
    } else {
      executeLargeInputsWithoutOutput(logins, l -> processLogins(bulk, dbSession, l));
    }
  }

  @Deprecated
  private void processLogins(BulkIndexer bulk, DbSession dbSession, @Nullable List<String> logins) {
    List<EsQueueDto> esQueueDtos = null;

    if (logins != null) {
      esQueueDtos = logins.stream()
        .map(l -> {
          EsQueueDto esQueueItem = EsQueueDto.create(EsQueueDto.Type.USER, l);
          dbClient.esQueueDao().insert(dbSession, esQueueItem);
          return esQueueItem;
        }).collect(toList());
      dbSession.commit();
    }

    try (UserResultSetIterator rowIt = UserResultSetIterator.create(dbClient, dbSession, esQueueDtos)) {
      processResultSet(dbSession, bulk, esQueueDtos, rowIt);
    }
  }

  private static void processResultSet(DbSession dbSession, BulkIndexer bulk, Collection<EsQueueDto> esQueueDtos, UserResultSetIterator users) {
    bulk.start(dbSession, esQueueDtos);
    while (users.hasNext()) {
      UserDoc user = users.next();
      // only index requests, no deletion requests.
      // Deactivated users are not deleted but updated.
      bulk.add(newIndexRequest(user));
    }
    bulk.stop();
  }

  private BulkIndexer newBulkIndexer(Size bulkSize) {
    return new BulkIndexer(dbClient, esClient, UserIndexDefinition.INDEX_TYPE_USER.getIndex(), bulkSize);
  }

  private static IndexRequest newIndexRequest(UserDoc user) {
    return new IndexRequest(UserIndexDefinition.INDEX_TYPE_USER.getIndex(), UserIndexDefinition.INDEX_TYPE_USER.getType(), user.getId())
      .source(user.getFields());
  }
}
