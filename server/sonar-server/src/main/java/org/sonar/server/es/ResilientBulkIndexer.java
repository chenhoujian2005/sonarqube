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

package org.sonar.server.es;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.sonar.db.DbClient;
import org.sonar.db.DbSession;
import org.sonar.db.es.EsQueueDto;

import static java.util.stream.Collectors.toList;

public class ResilientBulkIndexer {
  private final BulkIndexer bulkIndexer;
  private final DbClient dbClient;
  private final DbSession dbSession;
  private Collection<EsQueueDto> esQueueDtos;

  public ResilientBulkIndexer(DbClient dbClient, DbSession dbSession, EsClient client, String indexName, BulkIndexer.Size size) {
    this.bulkIndexer = new BulkIndexer(client, indexName, size);
    this.bulkIndexer.addResilientListener(new BulkProcessorListener());
    this.dbSession = dbSession;
    this.dbClient = dbClient;
  }

  public void start(Collection<EsQueueDto> esQueueDtos) {
    this.esQueueDtos = esQueueDtos;
    bulkIndexer.start();
  }

  public void stop() {
    bulkIndexer.stop();
  }

  public void add(ActionRequest<?> request) {
    bulkIndexer.add(request);
  }

  private final class BulkProcessorListener implements BulkProcessor.Listener {
    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
      // no action required
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse bulkResponse) {
      if (esQueueDtos != null) {
        List<EsQueueDto> itemsToDelete = Arrays.stream(bulkResponse.getItems())
          .filter(b -> !b.isFailed())
          .map(b -> esQueueDtos.stream().filter(t -> b.getId().equals(t.getDocUuid())).findFirst().orElse(null))
          .filter(Objects::nonNull)
          .collect(toList());

        dbClient.esQueueDao().delete(dbSession, itemsToDelete);
        dbSession.commit();
      }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest req, Throwable e) {
      // no action required
    }
  }
}
