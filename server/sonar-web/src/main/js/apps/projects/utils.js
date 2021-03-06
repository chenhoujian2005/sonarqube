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
// @flow
import { translate } from '../../helpers/l10n';

const DEFAULT_FILTER = 'sonarqube.projects.default';
const FAVORITE = 'favorite';
const ALL = 'all';

const VIEW = 'sonarqube.projects.view';
const VISUALIZATION = 'sonarqube.projects.visualization';
const SORT = 'sonarqube.projects.sort';

export const isFavoriteSet = (): boolean => {
  const setting = window.localStorage.getItem(DEFAULT_FILTER);
  return setting === FAVORITE;
};

export const isAllSet = (): boolean => {
  const setting = window.localStorage.getItem(DEFAULT_FILTER);
  return setting === ALL;
};

const save = (key: string, value: ?string) => {
  try {
    if (value) {
      window.localStorage.setItem(key, value);
    } else {
      window.localStorage.removeItem(key);
    }
  } catch (e) {
    // usually that means the storage is full
    // just do nothing in this case
  }
};

export const saveAll = () => save(DEFAULT_FILTER, ALL);

export const saveFavorite = () => save(DEFAULT_FILTER, FAVORITE);

export const saveView = (view: ?string) => save(VIEW, view);
export const getView = () => window.localStorage.getItem(VIEW);

export const saveVisualization = (visualization: ?string) => save(VISUALIZATION, visualization);
export const getVisualization = () => window.localStorage.getItem(VISUALIZATION);

export const saveSort = (sort: ?string) => save(SORT, sort);
export const getSort = () => window.localStorage.getItem(SORT);

export const SORTING_METRICS = [
  { value: 'name' },
  { value: 'analysis_date' },
  { value: 'reliability' },
  { value: 'security' },
  { value: 'maintainability' },
  { value: 'coverage' },
  { value: 'duplications' },
  { value: 'size' }
];

export const SORTING_LEAK_METRICS = [
  { value: 'name' },
  { value: 'analysis_date' },
  { value: 'new_reliability', class: 'projects-leak-sorting-option' },
  { value: 'new_security', class: 'projects-leak-sorting-option' },
  { value: 'new_maintainability', class: 'projects-leak-sorting-option' },
  { value: 'new_coverage', class: 'projects-leak-sorting-option' },
  { value: 'new_duplications', class: 'projects-leak-sorting-option' },
  { value: 'new_lines', class: 'projects-leak-sorting-option' }
];

export const SORTING_SWITCH = {
  analysis_date: 'analysis_date',
  name: 'name',
  reliability: 'new_reliability',
  security: 'new_security',
  maintainability: 'new_maintainability',
  coverage: 'new_coverage',
  duplications: 'new_duplications',
  size: 'new_lines',
  new_reliability: 'reliability',
  new_security: 'security',
  new_maintainability: 'maintainability',
  new_coverage: 'coverage',
  new_duplications: 'duplications',
  new_lines: 'size'
};

export const VIEWS = ['overall', 'leak'];

export const VISUALIZATIONS = [
  'risk',
  'reliability',
  'security',
  'maintainability',
  'coverage',
  'duplications'
];

export const localizeSorting = (sort?: string) => {
  return translate('projects.sort', sort || 'name');
};

export const parseSorting = (sort: string): { sortValue: string, sortDesc: boolean } => {
  const desc = sort[0] === '-';
  return { sortValue: desc ? sort.substr(1) : sort, sortDesc: desc };
};
