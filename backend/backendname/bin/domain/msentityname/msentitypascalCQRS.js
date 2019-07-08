"use strict";

const uuidv4 = require("uuid/v4");
const { of } = require("rxjs");
const { mergeMap, catchError, map, toArray, tap } = require('rxjs/operators');

const Event = require("@nebulae/event-store").Event;
const { CqrsResponseHelper } = require('@nebulae/backend-node-tools').cqrs;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const { CustomError, INTERNAL_SERVER_ERROR_CODE, PERMISSION_DENIED } = require("@nebulae/backend-node-tools").error;

const eventSourcing = require("../../tools/event-sourcing").eventSourcing;
const msentitypascalDA = require("./data-access/msentitypascalDA");

const READ_ROLES = ["PLATFORM-ADMIN"];
const WRITE_ROLES = ["PLATFORM-ADMIN"];
const REQUIRED_ATTRIBUTES = [];

/**
 * Singleton instance
 */
let instance;

class msentitypascalCQRS {
  constructor() {
  }

  /**     
   * Generates and returns an object that defines the CQRS request handlers.
   * 
   * The map is a relationship of: AGGREGATE_TYPE VS { MESSAGE_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
   * 
   * ## Example
   *  { "CreateUser" : { "somegateway.someprotocol.mutation.CreateUser" : {fn: createUser$, instance: classInstance } } }
   */
  generateRequestProcessorMap() {
    return {
      'msentitypascal': {
        "apiidcamellc.graphql.query.msnamepascalmsentitiespascal": { fn: instance.getmsentitypascalList$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "apiidcamellc.graphql.query.msnamepascalmsentitiespascalSize": { fn: instance.getmsentitypascalListSize$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "apiidcamellc.graphql.query.msnamepascalmsentitypascal": { fn: instance.getmsentitypascal$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "apiidcamellc.graphql.mutation.msnamepascalCreatemsentitypascal": { fn: instance.createmsentitypascal$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "apiidcamellc.graphql.mutation.msnamepascalUpdatemsentitypascalGeneralInfo": { fn: instance.updatemsentitypascalGeneralInfo$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "apiidcamellc.graphql.mutation.msnamepascalUpdatemsentitypascalState": { fn: instance.updatemsentitypascalState$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
      }
    }
  };

  /**  
   * Gets the msentitypascal
   *
   * @param {*} args args
   */
  getmsentitypascal$({ args }, authToken) {
    const { id } = args;
    return msentitypascalDA.getmsentitypascal$(id).pipe(
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => CqrsResponseHelper.handleError$(err))
    );
  }

  /**  
   * Gets the msentitypascal list
   *
   * @param {*} args args
   */
  getmsentitypascalList$({ args }, authToken) {
    const { filterInput, paginationInput } = args;
    return msentitypascalDA.getmsentitypascalList$(filterInput, paginationInput).pipe(
      toArray(),
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => CqrsResponseHelper.handleError$(err))
    );
  }

  /**  
 * Gets the amount of the msentitypascal according to the filter
 *
 * @param {*} args args
 */
  getmsentitypascalListSize$({ args }, authToken) {
    const { filterInput } = args;
    return msentitypascalDA.getmsentitypascalSize$(filterInput).pipe(
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => CqrsResponseHelper.handleError$(err))
    );
  }

  /**
  * Create a msentitypascal
  */
  createmsentitypascal$({ root, args, jwt }, authToken) {
    const aggregateId = uuidv4();
    const { input } = args;
    return eventSourcing.emitEvent$(
      new Event({
        eventType: "Created",
        eventTypeVersion: 1,
        aggregateType: 'msentitypascal',
        aggregateId,
        data: input,
        user: authToken.preferred_username
      })).pipe(
        map(() => ({ code: 200, message: `msentitypascal with id: ${aggregateId} has been created` })),
        mergeMap(r => CqrsResponseHelper.buildSuccessResponse$(r)),
        catchError(err => CqrsResponseHelper.handleError$(err))
      );
  }

  /**
 * Edit the msentitypascal state
 */
  updatemsentitypascalGeneralInfo$({ root, args, jwt }, authToken) {
    const { id, input } = args;

    return eventSourcing.emitEvent$(
      new Event({
        eventType: "GeneralInfoUpdated",
        eventTypeVersion: 1,
        aggregateType: 'msentitypascal',
        aggregateId: id,
        data: input,
        user: authToken.preferred_username
      })
    ).pipe(
      map(() => ({ code: 200, message: `msentitypascal with id: ${id} has been updated` })),
      mergeMap(r => CqrsResponseHelper.buildSuccessResponse$(r)),
      catchError(err => CqrsResponseHelper.handleError$(err))
    );
  }


  /**
   * Edit the msentitypascal state
   */
  updatemsentitypascalState$({ root, args, jwt }, authToken) {
    const { id, newState } = args;

    return eventSourcing.emitEvent$(
      new Event({
        eventType: "StateUpdated",
        eventTypeVersion: 1,
        aggregateType: 'msentitypascal',
        aggregateId: id,
        data: { state: newState },
        user: authToken.preferred_username
      })).pipe(
        map(() => ({ code: 200, message: `msentitypascal with id: ${id} has been updated` })),
        mergeMap(r => CqrsResponseHelper.buildSuccessResponse$(r)),
        catchError(err => CqrsResponseHelper.handleError$(err))
      );
  }
  //#endregion
}

/**
 * @returns {msentitypascalCQRS}
 */
module.exports = () => {
  if (!instance) {
    instance = new msentitypascalCQRS();
    ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
  }
  return instance;
};
