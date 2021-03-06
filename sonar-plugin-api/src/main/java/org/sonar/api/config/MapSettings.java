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
package org.sonar.api.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableMap;

/**
 * In-memory map-based implementation of {@link Settings}. It must be used
 * <b>only for unit tests</b>. This is not the implementation
 * deployed at runtime, so non-test code must never cast
 * {@link Settings} to {@link MapSettings}.
 *
 * @since 6.1
 */
public class MapSettings extends Settings {

  private final Map<String, String> props = new HashMap<>();

  public MapSettings() {
    super(new PropertyDefinitions(), new Encryption(null));
  }

  public MapSettings(PropertyDefinitions definitions) {
    super(definitions, new Encryption(null));
  }

  @Override
  protected Optional<String> get(String key) {
    return Optional.ofNullable(props.get(key));
  }

  @Override
  protected void set(String key, String value) {
    props.put(key, value);
  }

  @Override
  protected void remove(String key) {
    props.remove(key);
  }

  @Override
  public Map<String, String> getProperties() {
    return unmodifiableMap(props);
  }

  /**
   * Delete all properties
   */
  public MapSettings clear() {
    props.clear();
    return this;
  }
}
