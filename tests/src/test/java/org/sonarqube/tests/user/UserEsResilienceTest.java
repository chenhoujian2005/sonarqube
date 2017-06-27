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
package org.sonarqube.tests.user;

import com.sonar.orchestrator.Orchestrator;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.sonarqube.tests.Tester;
import org.sonarqube.ws.client.user.SearchRequest;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static util.ItUtils.expectHttpError;

public class UserEsResilienceTest {

  @ClassRule
  public static final Orchestrator orchestrator = Orchestrator.builderEnv()
    .setServerProperty("sonar.web.javaAdditionalOpts",
      format("-javaagent:%s=script:%s,boot:%s", findBytemanJar(), findBytemanScript(), findBytemanJar()))
    .setServerProperty("sonar.search.recovery.delayInSeconds", "1")
    .setServerProperty("sonar.search.recovery.minAgeInSeconds", "5")
    .build();

  @Rule
  public TestRule timeout = new DisableOnDebug(Timeout.builder()
    .withLookingForStuckThread(true)
    .withTimeout(60L, TimeUnit.SECONDS)
    .build());

  @Rule
  public Tester tester = new Tester(orchestrator);

  @Test
  public void creation_of_user_is_resilient() throws Exception {
    String login = "error";

    expectHttpError(500, "An error has occurred. Please contact your administrator",
      () -> tester.users().generate(u -> u.setLogin(login)));
    // user exists in db, it can't be created again.
    // However he's not indexed.
    expectHttpError(400, "An active user with login '" + login + "' already exists",
      () -> tester.users().generate(u -> u.setLogin(login)));
    assertThat(isUserReturnedInSearch("error")).isFalse();

    while (!isUserReturnedInSearch(login)) {
      // user is indexed by the recovery daemon, which runs every 5 seconds
      Thread.sleep(1_000L);
    }
  }

  private boolean isUserReturnedInSearch(String login) {
    return tester.users().service().search(SearchRequest.builder().setQuery(login).build()).getUsersCount() == 1L;
  }

  private static String findBytemanJar() {
    // see pom.xml, Maven copies and renames the artifact.
    File jar = new File("target/byteman.jar");
    if (!jar.exists()) {
      throw new IllegalStateException("Can't find " + jar + ". Please execute 'mvn generate-test-resources' on integration tests once.");
    }
    return jar.getAbsolutePath();
  }

  private static String findBytemanScript() {
    // see pom.xml, Maven copies and renames the artifact.
    File script = new File("../../tests/resilience/user_indexer.btm");
    if (!script.exists()) {
      throw new IllegalStateException("Can't find " + script);
    }
    return script.getAbsolutePath();
  }
}
