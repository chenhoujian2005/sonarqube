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
package org.sonar.db.es;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.sonar.api.utils.internal.TestSystem2;
import org.sonar.core.util.UuidFactoryFast;
import org.sonar.db.DbSession;
import org.sonar.db.DbTester;

import static org.assertj.core.api.Assertions.assertThat;

public class EsQueueDaoTest {

  private static TestSystem2 system2 = new TestSystem2();

  @Rule
  public DbTester dbTester = DbTester.create(system2);

  private DbSession dbSession = dbTester.getSession();
  private EsQueueDao underTest = dbTester.getDbClient().esQueueDao();

  @BeforeClass
  public static void setSystem2() {
    system2.setNow(1_000);
  }

  @Test
  public void insert_data() throws SQLException {
    int nbOfInsert = 10 + new Random().nextInt(20);
    List<EsQueueDto> esQueueDtos = new ArrayList<>();
    IntStream.rangeClosed(1, nbOfInsert).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);

    assertThat(dbTester.countSql(dbSession, "select count(*) from es_queue")).isEqualTo(nbOfInsert);
  }

  @Test
  public void delete_unknown_EsQueueDto_does_not_throw_exception() throws SQLException {
    int nbOfInsert = 10 + new Random().nextInt(20);
    List<EsQueueDto> esQueueDtos = new ArrayList<>();
    IntStream.rangeClosed(1, nbOfInsert).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);

    underTest.delete(dbSession, EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()));

    assertThat(dbTester.countSql(dbSession, "select count(*) from es_queue")).isEqualTo(nbOfInsert);
  }

  @Test
  public void delete_EsQueueDto_does_not_throw_exception() throws SQLException {
    int nbOfInsert = 10 + new Random().nextInt(20);
    List<EsQueueDto> esQueueDtos = new ArrayList<>();
    IntStream.rangeClosed(1, nbOfInsert).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);
    assertThat(dbTester.countSql(dbSession, "select count(*) from es_queue")).isEqualTo(nbOfInsert);

    underTest.delete(dbSession, esQueueDtos);

    assertThat(dbTester.countSql(dbSession, "select count(*) from es_queue")).isEqualTo(0);
  }

  @Test
  public void selectForRecovery_must_returns_10_000_when_there_is_more_rows() throws SQLException {
    int nbOfInsert = 10_000 + new Random().nextInt(100);
    system2.setNow(1_000L);
    List<EsQueueDto> esQueueDtos = new ArrayList<>();
    IntStream.rangeClosed(1, nbOfInsert).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);

    Collection<EsQueueDto> rowsToRecover = underTest.selectForRecovery(dbSession, 2_000, 0);

    assertThat(rowsToRecover).hasSize(10_000);
    rowsToRecover.stream().forEach(
      r -> assertThat(r.getDocType()).isEqualTo(EsQueueDto.Type.USER));
  }

  @Test
  public void selectForRecovery_must_returns_only_rows_between_limits() throws SQLException {
    List<EsQueueDto> esQueueDtos = new ArrayList<>();
    // Insert 100 rows before the selection
    system2.setNow(1_000L);
    IntStream.rangeClosed(1, 100).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);
    esQueueDtos.clear();

    // Insert rows for the selection
    system2.setNow(1_001L);
    int nbOfInsert = new Random().nextInt(100);
    IntStream.rangeClosed(1, nbOfInsert).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);
    esQueueDtos.clear();

    // Insert 100 rows after the selection
    system2.setNow(2_001L);
    IntStream.rangeClosed(1, 100).forEach(
      i -> esQueueDtos.add(EsQueueDto.create(EsQueueDto.Type.USER, UuidFactoryFast.getInstance().create()))
    );
    underTest.insert(dbSession, esQueueDtos);

    assertThat(underTest.selectForRecovery(dbSession, 1_000, 0))
      .hasSize(100);
    assertThat(underTest.selectForRecovery(dbSession, 2_000, 1001))
      .hasSize(nbOfInsert);
    assertThat(underTest.selectForRecovery(dbSession, 3_000, 2001))
      .hasSize(100);
  }
}
