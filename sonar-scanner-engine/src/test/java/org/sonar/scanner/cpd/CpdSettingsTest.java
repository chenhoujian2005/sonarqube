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
package org.sonar.scanner.cpd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.sonar.api.batch.bootstrap.ImmutableProjectDefinition;
import org.sonar.api.config.MapSettings;
import org.sonar.api.config.Settings;
import org.sonar.scanner.scan.ImmutableProjectReactor;

public class CpdSettingsTest {
  private CpdSettings cpdSettings;
  private Settings settings;
  private ImmutableProjectDefinition projectDefinition;
  
  @Before
  public void setUp() {
    ImmutableProjectReactor projectReactor = mock(ImmutableProjectReactor.class);
    when(projectReactor.getRoot()).thenReturn(projectDefinition);
    settings = new MapSettings();
    cpdSettings = new CpdSettings(settings, projectReactor);
  }
  
  @Test
  public void defaultMinimumTokens() {
    assertThat(cpdSettings.getMinimumTokens("java")).isEqualTo(100);
  }

  @Test
  public void minimumTokensByLanguage() {
    settings.setProperty("sonar.cpd.java.minimumTokens", "42");
    settings.setProperty("sonar.cpd.php.minimumTokens", "33");
    assertThat(cpdSettings.getMinimumTokens("java")).isEqualTo(42);

    settings.setProperty("sonar.cpd.java.minimumTokens", "42");
    settings.setProperty("sonar.cpd.php.minimumTokens", "33");
    assertThat(cpdSettings.getMinimumTokens("php")).isEqualTo(33);
  }
}
