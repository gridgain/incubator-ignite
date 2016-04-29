/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import template from './progress-overlay.jade!';
import './progress-overlay.css!';

export default ['igniteProgressOverlay', ['$progress', '$compile', ($progress, $compile) => {
    const compiledTemplate = $compile('<div class="progress-overlay progress-overlay_opacity-80 {{class}}"><div class="progress-overlay__wrapper"><div class="spinner"><div class="bounce1"></div><div class="bounce2"></div><div class="bounce3"></div></div><div class="progress-overlay__wellcome">{{ text }}</div></div></div>');

    const link = (scope, element, attrs) => {

        const build = () => {
            const progressOverlay = compiledTemplate(scope);
            if (!scope.progressOverlay) {
                scope.progressOverlay = progressOverlay;
                $progress.add(scope.key || 'defaultSpinnerKey', scope.progressOverlay);
                element.append(scope.progressOverlay);
            }
        };

        const update = () => {
            scope.progressOverlay && scope.progressOverlay.html(compiledTemplate(scope));
        };

        build();

        attrs.$observe('text', (value) => (_.isUndefined(value) && update()));

    };
    return {
        scope: {
            key: '@igniteProgressOverlay',
            text: '@?igniteProgressOverlayText',
            class: '@?igniteProgressOverlayClass'
        },
        restrict: 'A',
        link
    };
}]];
