////////// ANGULAR //////////
import {
  Component,
  OnInit,
  OnDestroy,
  ViewChild,
  ElementRef,
  Input
} from '@angular/core';

import {
  FormBuilder,
  FormGroup,
  FormControl,
  Validators
} from '@angular/forms';

import { Router, ActivatedRoute } from '@angular/router';

////////// RXJS ///////////
import {
  map,
  mergeMap,
  switchMap,
  toArray,
  filter,
  tap,
  takeUntil,
  startWith,
  debounceTime,
  distinctUntilChanged,
  take
} from 'rxjs/operators';

import { Subject, fromEvent, of, forkJoin, Observable, concat, combineLatest } from 'rxjs';

//////////// ANGULAR MATERIAL ///////////
import {
  MatPaginator,
  MatSort,
  MatTableDataSource,
  MatSnackBar,
  MatDialog
} from '@angular/material';

//////////// i18n ////////////
import {
  TranslateService
} from '@ngx-translate/core';
import { locale as english } from '../../i18n/en';
import { locale as spanish } from '../../i18n/es';
import { FuseTranslationLoaderService } from '../../../../../core/services/translation-loader.service';

//////////// Others ////////////
import { KeycloakService } from 'keycloak-angular';
import { msentitypascalDetailService } from '../msentityname-detail.service';
import { DialogComponent } from '../../dialog/dialog.component';
import { ToolbarService } from "../../../../toolbar/toolbar.service";

@Component({
  // tslint:disable-next-line:component-selector
  selector: 'msentityname-general-info',
  templateUrl: './msentityname-general-info.component.html',
  styleUrls: ['./msentityname-general-info.component.scss']
})
// tslint:disable-next-line:class-name
export class msentitypascalDetailGeneralInfoComponent implements OnInit, OnDestroy {
  // Subject to unsubscribe
  private ngUnsubscribe = new Subject();

  @Input('pageType') pageType: string;
  @Input('msentitycamel') msentitycamel: any;

  msentitycamelGeneralInfoForm: any;
  msentitycamelStateForm: any;

  timeoutMessage = null;

  constructor(
    private translationLoader: FuseTranslationLoaderService,
    private translate: TranslateService,
    private formBuilder: FormBuilder,
    public snackBar: MatSnackBar,
    private router: Router,
    private activatedRouter: ActivatedRoute,
    private msentitypascalDetailService: msentitypascalDetailService,
    private dialog: MatDialog,
    private toolbarService: ToolbarService
  ) {
      this.translationLoader.loadTranslations(english, spanish);
  }


  ngOnInit() {
    this.subscribeEventUpdated();
    this.msentitycamelGeneralInfoForm = new FormGroup({
      name: new FormControl(this.msentitycamel ? (this.msentitycamel.generalInfo || {}).name : ''),
      description: new FormControl(this.msentitycamel ? (this.msentitycamel.generalInfo || {}).description : '')
    });

    this.msentitycamelStateForm = new FormGroup({
      state: new FormControl(this.msentitycamel ? this.msentitycamel.state : true)
    });
  }

  subscribeEventUpdated(){
    this.msentitypascalDetailService.notifymsentitypascalUpdatedSubscription$
    .pipe(
      takeUntil(this.ngUnsubscribe)
    )
    .subscribe(data => {
      if(this.timeoutMessage){
        clearTimeout(this.timeoutMessage);
      }      
    })
  }

  createmsentitypascal() {
    this.showConfirmationDialog$("msnameuppercase.CREATE_MESSAGE", "msnameuppercase.CREATE_TITLE")
        .pipe(
          tap(ok => this.showWaitOperationMessage()), 
          mergeMap(ok => {
            this.msentitycamel = {
              generalInfo: this.msentitycamelGeneralInfoForm.getRawValue(),
              state: this.msentitycamelStateForm.getRawValue().state
            };
            this.msentitycamel.generalInfo.name = this.msentitycamel.generalInfo.name.toUpperCase();
            return this.msentitypascalDetailService.createmsnamepascalmsentitypascal$(this.msentitycamel);
          }),
          mergeMap(resp => this.graphQlAlarmsErrorHandler$(resp)),
          filter((resp: any) => !resp.errors || resp.errors.length === 0),          
          takeUntil(this.ngUnsubscribe)
        ).subscribe(result => {},
          error => {
            this.showErrorOperationMessage();
            console.log('Error ==> ', error);
          }
      );
  }

  updatemsentitypascalGeneralInfo() {
    this.showConfirmationDialog$("msnameuppercase.UPDATE_MESSAGE", "msnameuppercase.UPDATE_TITLE")
      .pipe(
        tap(ok => this.showWaitOperationMessage()), 
        mergeMap(ok => {
          const generalInfoinput = {
            name: this.msentitycamelGeneralInfoForm.getRawValue().name.toUpperCase(),
            description: this.msentitycamelGeneralInfoForm.getRawValue().description
          };
          return this.msentitypascalDetailService.updatemsnamepascalmsentitypascalGeneralInfo$(this.msentitycamel._id, generalInfoinput);
        }),
        mergeMap(resp => this.graphQlAlarmsErrorHandler$(resp)),
        filter((resp: any) => !resp.errors || resp.errors.length === 0),
        takeUntil(this.ngUnsubscribe)
      )
      .subscribe(result => {},
        error => {
          this.showErrorOperationMessage();
          console.log('Error ==> ', error);
        }
      );

  }

  onmsentitypascalStateChange() {
    this.showConfirmationDialog$("msnameuppercase.UPDATE_MESSAGE", "msnameuppercase.UPDATE_TITLE")
      .pipe(
        tap(ok => this.showWaitOperationMessage()), 
        mergeMap(ok => {        
          return this.msentitypascalDetailService.updatemsnamepascalmsentitypascalState$(this.msentitycamel._id, this.msentitycamelStateForm.getRawValue().state);
        }),
        mergeMap(resp => this.graphQlAlarmsErrorHandler$(resp)),
        filter((resp: any) => !resp.errors || resp.errors.length === 0),
        takeUntil(this.ngUnsubscribe)
      ).subscribe(result => {},
        error => {
          this.showErrorOperationMessage();
          console.log('Error ==> ', error);
        });
  }

  showWaitOperationMessage(){
    this.timeoutMessage = setTimeout(() => {
      this.showSnackBar('msnameuppercase.WAIT_OPERATION');
    }, 2000);
  }

  showErrorOperationMessage(){
    if(this.timeoutMessage){
      clearTimeout(this.timeoutMessage);
    }
    this.showSnackBar('msnameuppercase.ERROR_OPERATION');
  }

  showConfirmationDialog$(dialogMessage, dialogTitle) {
    return this.dialog
      //Opens confirm dialog
      .open(DialogComponent, {
        data: {
          dialogMessage,
          dialogTitle
        }
      })
      .afterClosed()
      .pipe(
        filter(okButton => okButton),
      );
  }

  showSnackBar(message) {
    this.snackBar.open(this.translationLoader.getTranslate().instant(message),
      this.translationLoader.getTranslate().instant('msnameuppercase.CLOSE'), {
        duration: 6000
      });
  }

  graphQlAlarmsErrorHandler$(response) {
    return of(JSON.parse(JSON.stringify(response)))
      .pipe(
        tap((resp: any) => {
          this.showSnackBarError(resp);

          return resp;
        })
      );
  }

  /**
   * Shows an error snackbar
   * @param response
   */
  showSnackBarError(response) {
    if (response.errors) {
      if(this.timeoutMessage){
        clearTimeout(this.timeoutMessage);
      }
      if (Array.isArray(response.errors)) {
        response.errors.forEach(error => {
          if (Array.isArray(error)) {
            error.forEach(errorDetail => {
              this.showMessageSnackbar('ERRORS.' + errorDetail.message.code);
            });
          } else {
            response.errors.forEach(error => {
              this.showMessageSnackbar('ERRORS.' + error.message.code);
            });
          }
        });
      }
    }
  }

  /**
   * Shows a message snackbar on the bottom of the page
   * @param messageKey Key of the message to i18n
   * @param detailMessageKey Key of the detail message to i18n
   */
  showMessageSnackbar(messageKey, detailMessageKey?) {
    let translationData = [];
    if (messageKey) {
      translationData.push(messageKey);
    }

    if (detailMessageKey) {
      translationData.push(detailMessageKey);
    }

    this.translate.get(translationData)
      .subscribe(data => {
        this.snackBar.open(
          messageKey ? data[messageKey] : '',
          detailMessageKey ? data[detailMessageKey] : '',
          {
            duration: 2000
          }
        );
      });
  }



  ngOnDestroy() {
    this.ngUnsubscribe.next();
    this.ngUnsubscribe.complete();
  }

}
