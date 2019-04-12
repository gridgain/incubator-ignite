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

import {ReplaySubject} from 'rxjs';
import {StateService} from '@uirouter/angularjs';
import {default as MessagesFactory} from 'app/services/Messages.service';

export type User = {
    _id: string,
    admin: boolean,
    country: string,
    email: string,
    phone?: string,
    company?: string,
    firstName: string,
    lastName: string,
    lastActivity: string,
    lastLogin: string,
    registered: string,
    token: string
}

User.$inject = ['$q', '$injector', '$rootScope', '$state', '$http', 'IgniteMessages'];

export default function User(
    $q: ng.IQService,
    $injector: ng.auto.IInjectorService,
    $root: ng.IRootScopeService,
    $state: StateService,
    $http: ng.IHttpService,
    IgniteMessages: ReturnType<typeof MessagesFactory>
) {
    let user: ng.IPromise<User>;

    const current$ = new ReplaySubject(1);

    return {
        current$,
        /**
         * @returns {ng.IPromise<User>}
         */
        load() {
            return user = $http.post('/api/v1/user')
                .then(({data}) => {
                    $root.user = data;

                    $root.$broadcast('user', $root.user);

                    current$.next(data);

                    return $root.user;
                })
                .catch(({data}) => {
                    user = null;

                    return $q.reject(data);
                });
        },
        read() {
            if (user)
                return user;

            return this.load();
        },
        clean() {
            delete $root.user;

            delete $root.IgniteDemoMode;

            sessionStorage.removeItem('IgniteDemoMode');
        },

        async save(user: Partial<User>): Promise<User> {
            try {
                const {data: updatedUser} = await $http.post<User>('/api/v1/profile/save', user);
                await this.load();
                IgniteMessages.showInfo('Profile saved.');
                $root.$broadcast('user', updatedUser);
                return updatedUser;
            } catch (e) {
                IgniteMessages.showError('Failed to save profile: ', e);
            }
        }
    };
}
