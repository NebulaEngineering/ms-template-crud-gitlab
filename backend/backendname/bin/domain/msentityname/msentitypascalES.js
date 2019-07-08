'use strict'

const { of } = require("rxjs");
const { mergeMap, tap } = require('rxjs/operators');

const { brokerFactory } = require("@nebulae/backend-node-tools").broker;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const broker = brokerFactory();
const msentitypascalDA = require('./data-access/msentitypascalDA');
const MATERIALIZED_VIEW_TOPIC = process.env.EMI_MATERIALIZED_VIEW_UPDATES_TOPIC;
/**
 * Singleton instance
 */
let instance;

class msentitypascalES {

    constructor() {
    }

    /**     
     * Generates and returns an object that defines the Event-Sourcing events handlers.
     * 
     * The map is a relationship of: AGGREGATE_TYPE VS { EVENT_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
     * 
     * ## Example
     *  { "User" : { "UserAdded" : {fn: handleUserAdded$, instance: classInstance } } }
     */
    generateEventProcessorMap() {
        return {
            'msentitypascal': {
                "Created": { fn: instance.handlemsentitypascalCreated$, instance },
                "GeneralInfoUpdated": { fn: instance.handlemsentitypascalGeneralInfoUpdated$, instance },
                "StateUpdated": { fn: instance.handlemsentitypascalStateUpdated$, instance },
            }
        }
    };

    /**
     * Persists the msentitypascal on the materialized view according to the received data from the event store.
     * @param {*} msentitypascalCreatedEvent msentitypascal created event
     */
    handlemsentitypascalCreated$({ aid, data, user, timestamp }) {
        const msentitypascal = {
            ...data,
            creatorUser: user,
            creationTimestamp: timestamp,
            modifierUser: user,
            modificationTimestamp: timestamp,
            _id: aid,
        };
        return msentitypascalDA.createmsentitypascal$(msentitypascal).pipe(
            mergeMap(result => broker.send$(MATERIALIZED_VIEW_TOPIC, `msentitypascalUpdatedSubscription`, result.ops[0]))
        );
    }

    /**
     * Update the general info on the materialized view according to the received data from the event store.
     * @param {*} msentitypascalGeneralInfoUpdatedEvent msentitypascal created event
     */
    handlemsentitypascalGeneralInfoUpdated$({ aid, data, user, timestamp }) {
        return msentitypascalDA.updatemsentitypascalGeneralInfo$(aid, data, user, timestamp).pipe(
            mergeMap(result => broker.send$(MATERIALIZED_VIEW_TOPIC, `msentitypascalUpdatedSubscription`, result))
        );
    }

    /**
     * updates the state on the materialized view according to the received data from the event store.
     * @param {*} msentitypascalStateUpdatedEvent events that indicates the new state of the msentitypascal
     */
    handlemsentitypascalStateUpdated$({ aid, data, user, timestamp }) {
        const { state } = data;
        return msentitypascalDA.updatemsentitypascalState$(aid, state, user, timestamp).pipe(
            mergeMap(result => broker.send$(MATERIALIZED_VIEW_TOPIC, `msentitypascalUpdatedSubscription`, result))
        );
    }

}


/**
 * @returns {msentitypascalES}
 */
module.exports = () => {
    if (!instance) {
        instance = new msentitypascalES();
        ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};