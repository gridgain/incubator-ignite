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

import 'core-js/es7/reflect';
import 'zone.js/dist/zone';
import './style.scss';

import {NgModule, Inject} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {UIRouterUpgradeModule} from '@uirouter/angular-hybrid';
import { UIRouter } from '@uirouter/angular';
import { UpgradeModule } from '@angular/upgrade/static';
import {NgxPopperModule} from 'ngx-popper';

import {ServiceBootstrapComponent} from './components/serviceBootstrap';
export {ServiceBootstrapComponent};
import {PageProfile} from './components/page-profile/component';
export {PageProfile};
import {IgniteIcon} from './components/igniteIcon.component';
import {FormFieldTooltip} from './components/formFieldTooltip.component';

import {ReactiveFormsModule} from '@angular/forms';

export const declarations = [
    ServiceBootstrapComponent,
    PageProfile,
    IgniteIcon,
    FormFieldTooltip
];

export const entryComponents = [
    ServiceBootstrapComponent,
    PageProfile
];

export const providers = [
    {
        provide: 'IgniteCountries',
        useFactory: (i) => i.get('IgniteCountries'),
        deps: ['$injector']
    },
    {
        provide: 'User',
        useFactory: (i) => i.get('User'),
        deps: ['$injector']
    },
    {
        provide: 'IgniteIcons',
        useFactory: (i) => i.get('IgniteIcons'),
        deps: ['$injector']
    }
];

import {states} from './states';

@NgModule({
    imports: [
        BrowserModule,
        ReactiveFormsModule,
        UpgradeModule,
        UIRouterUpgradeModule.forRoot({states}),
        NgxPopperModule.forRoot({
            applyClass: 'ignite-popper',
            appendTo: 'body',
            boundariesElement: 'ui-view.content'
        })
    ],
    providers,
    declarations,
    entryComponents
})
export class IgniteWebConsoleModule {
    static parameters = [[new Inject(UIRouter)]]
    constructor(private router: UIRouter) {}
    ngDoBootstrap() {}
}
