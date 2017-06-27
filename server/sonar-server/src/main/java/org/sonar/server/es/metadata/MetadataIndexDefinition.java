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
package org.sonar.server.es.metadata;

import org.sonar.api.config.Settings;
import org.sonar.server.es.IndexDefinition.IndexDefinitionContext;
import org.sonar.server.es.IndexType;
import org.sonar.server.es.NewIndex;

public class MetadataIndexDefinition {

  public static final IndexType INDEX_TYPE_METADATA = new IndexType("metadatas", "metadata");
  public static final String FIELD_VALUE = "value";

  private static final int DEFAULT_NUMBER_OF_SHARDS = 1;

  private final Settings settings;

  public MetadataIndexDefinition(Settings settings) {
    this.settings = settings;
  }

  public void define(IndexDefinitionContext context) {
    NewIndex index = context.create(INDEX_TYPE_METADATA.getIndex());
    index.refreshHandledByIndexer();
    index.configureShards(settings, DEFAULT_NUMBER_OF_SHARDS);

    NewIndex.NewIndexType mapping = index.createType(INDEX_TYPE_METADATA.getType());

    mapping.keywordFieldBuilder(FIELD_VALUE).disableSearch().build();
  }
}
