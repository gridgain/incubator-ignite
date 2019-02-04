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

import {TemplateRef, AfterViewInit, Injectable, ContentChild, ContentChildren, Inject, Component, Directive, ViewEncapsulation, ViewChild, HostBinding, ElementRef, Host, Optional, Input} from '@angular/core';
import {FormControlDirective, FormControlName, FormControl} from '@angular/forms';
import './formField.component.scss';
import {PopperContent} from 'ngx-popper';

export enum FormFieldRequiredMarkerStyles {
    OPTIONAL,
    REQUIRED
}

export enum FormFieldErrorStyles {
    INLINE = 'inline',
    ICON = 'icon'
}

@Injectable({
    providedIn: 'root'
})
export class FORM_FIELD_OPTIONS {
    requiredMarkerStyle: FormFieldRequiredMarkerStyles = FormFieldRequiredMarkerStyles.REQUIRED
    errorStyle: FormFieldErrorStyles = FormFieldErrorStyles.ICON
}

@Injectable({
    providedIn: 'root'
})
export class VALIDATION_MESSAGES {
    required = 'Value is required'
    email = 'Email has invalid format'
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

@Component({
    selector: 'form-field-errors',
    template: `
        <ng-template #validationMessage>
            <ng-template *ngIf='extraErrorMessages[errorType]' [ngTemplateOutlet]='extraErrorMessages[errorType]'></ng-template>
            <ng-container *ngIf='!extraErrorMessages[errorType] && defaultMessages[errorType]'>{{defaultMessages[errorType]}}</ng-container>
            <ng-container *ngIf='!extraErrorMessages[errorType] && !defaultMessages[errorType]'>Value is invalid: {{errorType}}</ng-container>
        </ng-template>
        <div *ngIf='errorStyle === "inline"' class='inline'>
            <ng-container *ngTemplateOutlet='validationMessage'></ng-container>
        </div>
        <div *ngIf='errorStyle === "icon"' class='icon'>
            <popper-content #popper>
                <ng-container *ngTemplateOutlet='validationMessage'></ng-container>
            </popper-content>
            <ignite-icon
                name='attention'
                [popper]='popper'
                popperApplyClass='ignite-popper,ignite-popper__error'
                popperTrigger='hover'
                popperPlacement='top'
                popperAppendTo='body'
            ></ignite-icon>
        </div>
    `,
    styles: [`
        :host {
            display: block;
        }
        .inline {
            padding: 5px 10px 0px;
            color: #ee2b27;
            font-size: 12px;
            line-height: 14px;
        }
        .icon {
            color: #ee2b27;
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
        }
    `]
})
export class FormFieldErrors<T extends {[errorType: string]: string}> {
    @Input()
    errorStyle: FormFieldErrorStyles
    @Input()
    extraErrorMessages: T = {} as T
    @Input()
    errorType: keyof T
    static parameters = [[new Inject(VALIDATION_MESSAGES)]]
    constructor(private defaultMessages: VALIDATION_MESSAGES) {}
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
        <form-field-errors
            *ngIf='(control.dirty || control.touched) && control.invalid'
            [errorStyle]='options.errorStyle'
            [errorType]='_getErrorType(control.control)'
            [extraErrorMessages]='extraMessages'
        ></form-field-errors>
    `
})
export class FormField implements AfterViewInit {
    static parameters = [[new Inject(FORM_FIELD_OPTIONS)]]
    constructor(private options: FORM_FIELD_OPTIONS) {}

    extraMessages = {}

    @ContentChild(FormControlName)
    control: FormControlName

    @ContentChild(FormFieldHint)
    hint: FormFieldHint

    @HostBinding('class.form-field__required')
    isRequired: boolean
    @HostBinding('class.form-field__optional')
    isOptional: boolean
    @HostBinding('class.form-field__icon-error')
    get isIconError() {
        return this.options.errorStyle === FormFieldErrorStyles.ICON;
    }
    @HostBinding('class.form-field__inline-error')
    get isInlineError() {
        return this.options.errorStyle === FormFieldErrorStyles.INLINE;
    }

    ngAfterViewInit() {
        const hasRequired: boolean = this.control && this.control.control.validator && this.control.control.validator({}).required;
        this.isOptional = this.options.requiredMarkerStyle === FormFieldRequiredMarkerStyles.OPTIONAL && !hasRequired;
        this.isRequired = this.options.requiredMarkerStyle === FormFieldRequiredMarkerStyles.REQUIRED && hasRequired;
    }

    _getErrorType(control: FormControl): string {
        return control.errors ? Object.entries(control.errors).filter(([key, invalid]) => invalid).map(([key]) => key).pop() : void 0;
    }

    addExtraErrorMessage(key, message) {
        this.extraMessages = {
            ...this.extraMessages,
            [key]: message
        };
    }
}

@Component({
    selector: 'form-field-error',
    template: `<ng-template #errorTemplate><ng-content></ng-content></ng-template>`
})
export class FormFieldError implements AfterViewInit {
    @Input()
    error: string

    @ViewChild('errorTemplate')
    template: TemplateRef<any>

    static parameters = [[new Inject(ElementRef)], [new Inject(FormField)]]
    constructor(private ref: ElementRef, private formField: FormField) {}
    ngAfterViewInit() {
        this.formField.addExtraErrorMessage(this.error, this.template);
    }
}



export const FormFieldComponents = [FormFieldHint, FormFieldErrors, FormFieldError, FormField];
