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
package org.sonar.scanner.scan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import org.sonar.api.batch.bootstrap.ProjectReactor;
import org.sonar.api.config.ImmutableSettings;
import org.sonar.api.utils.MessageException;
import org.sonar.scanner.analysis.DefaultAnalysisMode;
import org.sonar.scanner.bootstrap.GlobalSettings;
import org.sonar.scanner.repository.ProjectRepositories;

@Immutable
public class ProjectSettings extends ImmutableSettings {

  private final GlobalSettings globalSettings;
  private final ProjectRepositories projectRepositories;
  private final DefaultAnalysisMode mode;
  private final Map<String, String> properties;

  public ProjectSettings(ProjectReactor reactor, GlobalSettings globalSettings, ProjectRepositories projectRepositories, DefaultAnalysisMode mode) {
    super(globalSettings.getDefinitions(), globalSettings.getEncryption());
    this.mode = mode;
    this.globalSettings = globalSettings;
    this.projectRepositories = projectRepositories;
    this.properties = Collections.unmodifiableMap(init(reactor));
  }

  private Map<String, String> init(ProjectReactor reactor) {
    Map<String, String> props = new HashMap<>();
    addProperties(globalSettings.getProperties(), props);
    addProperties(projectRepositories.settings(reactor.getRoot().getKeyWithBranch()), props);
    addProperties(reactor.getRoot().properties(), props);
    return props;
  }

  @Override
  protected Optional<String> get(String key) {
    if (mode.isIssues() && key.endsWith(".secured") && !key.contains(".license")) {
      throw MessageException.of("Access to the secured property '" + key
        + "' is not possible in issues mode. The SonarQube plugin which requires this property must be deactivated in issues mode.");
    }
    return Optional.ofNullable(properties.get(key));
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}
