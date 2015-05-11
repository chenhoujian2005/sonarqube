/*
 * SonarQube, open source software quality management tool.
 * Copyright (C) 2008-2014 SonarSource
 * mailto:contact AT sonarsource DOT com
 *
 * SonarQube is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * SonarQube is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.sonar.server.plugins.ws;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.sonar.api.server.ws.Request;
import org.sonar.api.server.ws.WebService;
import org.sonar.core.permission.GlobalPermissions;
import org.sonar.server.exceptions.ForbiddenException;
import org.sonar.server.plugins.PluginDownloader;
import org.sonar.server.plugins.UpdateCenterMatrixFactory;
import org.sonar.server.tester.UserSessionRule;
import org.sonar.server.ws.WsTester;
import org.sonar.updatecenter.common.Plugin;
import org.sonar.updatecenter.common.PluginUpdate;
import org.sonar.updatecenter.common.PluginUpdate.Status;
import org.sonar.updatecenter.common.Release;
import org.sonar.updatecenter.common.UpdateCenter;
import org.sonar.updatecenter.common.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdatePluginsWsActionTest {
  private static final String DUMMY_CONTROLLER_KEY = "dummy";
  private static final String CONTROLLER_KEY = "api/plugins";
  private static final String ACTION_KEY = "update";
  private static final String KEY_PARAM = "key";
  private static final String PLUGIN_KEY = "pluginKey";

  @Rule
  public UserSessionRule userSessionRule = UserSessionRule.standalone();

  private UpdateCenterMatrixFactory updateCenterFactory = mock(UpdateCenterMatrixFactory.class);
  private UpdateCenter updateCenter = mock(UpdateCenter.class);
  private PluginDownloader pluginDownloader = mock(PluginDownloader.class);
  private UpdatePluginsWsAction underTest = new UpdatePluginsWsAction(updateCenterFactory, pluginDownloader, userSessionRule);

  private WsTester wsTester = new WsTester(new PluginsWs(underTest));
  private Request invalidRequest = wsTester.newGetRequest(CONTROLLER_KEY, ACTION_KEY);
  private Request validRequest = wsTester.newGetRequest(CONTROLLER_KEY, ACTION_KEY).setParam(KEY_PARAM, PLUGIN_KEY);
  private WsTester.TestResponse response = new WsTester.TestResponse();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    when(updateCenterFactory.getUpdateCenter(anyBoolean())).thenReturn(updateCenter);

    userSessionRule.setGlobalPermissions(GlobalPermissions.SYSTEM_ADMIN);
  }

  @Test
  public void user_must_have_system_admin_permission() throws Exception {
    expectedException.expect(ForbiddenException.class);
    expectedException.expectMessage("Insufficient privileges");

    // no permission on user
    userSessionRule.setGlobalPermissions();

    underTest.handle(validRequest, response);
  }

  @Test
  public void action_update_is_defined() {
    WsTester wsTester = new WsTester();
    WebService.NewController newController = wsTester.context().createController(DUMMY_CONTROLLER_KEY);

    underTest.define(newController);
    newController.done();

    WebService.Controller controller = wsTester.controller(DUMMY_CONTROLLER_KEY);
    assertThat(controller.actions()).extracting("key").containsExactly(ACTION_KEY);

    WebService.Action action = controller.actions().iterator().next();
    assertThat(action.isPost()).isTrue();
    assertThat(action.description()).isNotEmpty();
    assertThat(action.responseExample()).isNull();

    assertThat(action.params()).hasSize(1);
    WebService.Param key = action.param(KEY_PARAM);
    assertThat(key).isNotNull();
    assertThat(key.isRequired()).isTrue();
    assertThat(key.description()).isNotNull();
  }

  @Test
  public void IAE_is_raised_when_key_param_is_not_provided() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Parameter 'key' is missing");

    underTest.handle(invalidRequest, response);
  }

  @Test
  public void IAE_is_raised_when_there_is_no_plugin_update_for_the_key() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("No plugin with key 'pluginKey'");

    underTest.handle(validRequest, response);
  }

  @Test
  public void if_plugin_has_an_update_download_is_triggered_with_latest_version_from_updatecenter() throws Exception {
    Version version = Version.create("1.0");
    when(updateCenter.findPluginUpdates()).thenReturn(ImmutableList.of(
      PluginUpdate.createWithStatus(new Release(new Plugin(PLUGIN_KEY), version), Status.COMPATIBLE)
      ));

    underTest.handle(validRequest, response);

    verify(pluginDownloader).download(PLUGIN_KEY, version);
    assertThat(response.outputAsString()).isEmpty();
  }

}
