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

import {AfterViewInit, Injectable, ContentChild, Inject, Component, Directive, ViewEncapsulation, ViewChild, HostBinding, ElementRef, Host, Optional, Input} from '@angular/core';
import {FormControlDirective, FormControlName} from '@angular/forms';
import './formField.component.scss';
import {PopperContent} from 'ngx-popper';

export enum FormFieldRequiredMarkerStyles {
    OPTIONAL,
    REQUIRED
}

export enum FormFielErrorStyles {
    INLINE,
    ICON
}

@Injectable({
    providedIn: 'root'
})
export class FORM_FIELD_OPTIONS {
    requiredMarkerStyle: FormFieldRequiredMarkerStyles = FormFieldRequiredMarkerStyles.REQUIRED
    errorStyle: FormFielErrorStyles = FormFielErrorStyles.ICON
}

@Component({
    selector: 'form-field-hint',
    template: `
        <popper-content>
            <ng-content></ng-content>
        </popper-content>
    `
})
class FormFieldHint {
    @ViewChild(PopperContent)
    popper: PopperContent
}

enum FormFieldRequiredClassname {
    REQUIRED = 'form-field__required',
    OPTIONAL = 'form-field__optional'
}

@Component({
    selector: 'form-field',
    template: `
        <div class="angular-form-field__label">
            <ng-content select="label"></ng-content>
            <form-field-tooltip *ngIf='hint' [content]='hint.popper'></form-field-tooltip>
        </div>
        <div class="angular-form-field__input">
            <ng-content select='input,select'></ng-content>
        </div>
    `,
    styles: [`
        :host {

        }
    `]
    // encapsulation: ViewEncapsulation.ShadowDom
})
export class FormField implements AfterViewInit {
    static parameters = [[new Inject(FORM_FIELD_OPTIONS)]]
    constructor(private options: FORM_FIELD_OPTIONS) {}

    @ContentChild(FormControlName)
    control: FormControlName

    @ContentChild(FormFieldHint)
    hint: FormFieldHint

    @HostBinding('class.form-field__required')
    isRequired: boolean
    @HostBinding('class.form-field__optional')
    isOptional: boolean

    // @ContentChild()

    ngAfterViewInit() {
        const hasRequired: boolean = this.control && this.control.control.validator && this.control.control.validator({}).required;
        this.isOptional = this.options.requiredMarkerStyle === FormFieldRequiredMarkerStyles.OPTIONAL && !hasRequired;
        this.isRequired = this.options.requiredMarkerStyle === FormFieldRequiredMarkerStyles.REQUIRED && hasRequired;
    }

}

export const FormFieldComponents = [FormFieldHint, FormField];
