"use strict";

const { from, concat, forkJoin } = require("rxjs");
const { map, filter, mergeMap, concatMap, last } = require('rxjs/operators');
const { eventSourcing } = require("../../tools").EventSourcing;
const { eventSourcingProcessorMaps } = require("../../domain");
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
/**
 * Singleton instance
 */
let instance;
/**
 * Micro-BackEnd key
 */
const mbeKey = process.env.MICROBACKEND_KEY;

class EventStoreService {
  constructor() {
    this.eventsProcessMap = this.joinEventsProcessMap();
    this.subscriptions = [];
  }

  /**
   * Starts listening to the EventStore
   * Returns observable that resolves to each subscribe agregate/event
   *    emit value: { aggregateType, eventType, handlerName}
   */
  start$() {
    //default error handler
    const onErrorHandler = error => {
      ConsoleLogger.e("Error handling EventStore incoming event", error);
      process.exit(1);
    };
    //default onComplete handler
    const onCompleteHandler = () => {
      () => ConsoleLogger.e("EventStore incoming event subscription completed");
    };
    ConsoleLogger.i("EventStoreService starting ...");

    return from(Object.keys(this.eventsProcessMap)).pipe(
      map((aggregateType) => this.subscribeEventHandler({ aggregateType, onErrorHandler, onCompleteHandler }))
    );
  }

  /**
   * Stops listening to the Event store
   * Returns observable that resolves to each unsubscribed subscription as string
   */
  stop$() {
    return from(this.subscriptions).pipe(
      map(subscription => {
        subscription.subscription.unsubscribe();
        return `Unsubscribed: aggregateType=${aggregateType}, eventType=${eventType}, handlerName=${handlerName}`;
      })
    );
  }

  /**
     * Create a subscrition to the event store and returns the subscription info     
     * @param {{ aggregateType: string, onErrorHandler, onCompleteHandler  }} params
     * @return { aggregateType  }
     */
  subscribeEventHandler({ aggregateType, onErrorHandler, onCompleteHandler }) {
    const subscription =
      //MANDATORY:  AVOIDS ACK REGISTRY DUPLICATIONS
      eventSourcing.ensureAcknowledgeRegistry$(aggregateType, mbeKey).pipe(
        mergeMap(() => eventSourcing.getEventListener$(aggregateType, mbeKey, false)),
        filter(event => this.eventsProcessMap[aggregateType][event.et]),
        map(event => ({ event, handlers: this.eventsProcessMap[aggregateType][event.et] })),
        map(({ event, handlers }) => ({
          handlerObservables: handlers.map(({ fn, instance }) => fn.call(instance, event)),
          event
        })),
        mergeMap(({ event, handlerObservables }) =>
          concat(
            forkJoin(...handlerObservables),
          ).pipe(
            mergeMap(() =>
              //MANDATORY:  ACKWOWLEDGE THIS EVENT WAS PROCESSED
              eventSourcing.acknowledgeEvent$(event, mbeKey))
          )
        ),
      ).subscribe(
        ({ at, aid, et, av }) => {
          ConsoleLogger.d(`EventStoreService.subscribeEventHandler: at:${at}, et:${et}, aid:${aid}, av:${av}`);
        },
        onErrorHandler,
        onCompleteHandler
      );
    this.subscriptions.push({ aggregateType, subscription });
    return `EventStoreService.subscribeEventHandler: aggregateType:${aggregateType}, events:${Object.keys(this.eventsProcessMap[aggregateType])}`;
  }

  /**
  * Starts listening to the EventStore
  * Returns observable that resolves to each subscribe agregate/event
  *    emit value: { aggregateType, eventType, handlerName}
  */
  syncState$() {
    return from(Object.keys(this.eventsProcessMap)).pipe(
      concatMap(aggregateType => this.subscribeEventRetrieval$(aggregateType))
    );
  }


  /**
   * Create a subscrition to the event store and returns the subscription info     
   * @param {{aggregateType, eventType, onErrorHandler, onCompleteHandler}} params
   * @return { aggregateType, eventType, handlerName  }
   */
  subscribeEventRetrieval$(aggregateType) {
    //MANDATORY:  AVOIDS ACK REGISTRY DUPLICATIONS
    return eventSourcing.ensureAcknowledgeRegistry$(aggregateType, mbeKey).pipe(
      mergeMap(() => eventSourcing.retrieveUnacknowledgedEvents$(aggregateType, mbeKey)),
      filter(event => this.eventsProcessMap[aggregateType][event.et]),
      map(event => ({ event, handlers: this.eventsProcessMap[aggregateType][event.et] })),
      map(({ event, handlers }) => ({
        handlerObservables: handlers.map(({ fn, instance }) => fn.call(instance, event)),
        event
      })),
      mergeMap(({ event, handlerObservables }) =>
        concat(
          forkJoin(...handlerObservables),
        ).pipe(
          mergeMap(() =>
            //MANDATORY:  ACKWOWLEDGE THIS EVENT WAS PROCESSED
            eventSourcing.acknowledgeEvent$(event, mbeKey))
        )
      ),
    );
  }


  /**
   * Joins all the event processors maps in the domain
   * @return {*} joined map -> { AGGREGATE_TYPE vs { EVENT_TYPE vs [ {fn: HANDLER_FN, instance: HANDLER_INSTANCE} ] } }
   */
  joinEventsProcessMap() {
    return eventSourcingProcessorMaps.reduce(
      (acc, eventSourcingProcessorMap) => {
        Object.keys(eventSourcingProcessorMap).forEach(AggregateType => {
          if (!acc[AggregateType]) { acc[AggregateType] = {} }
          Object.keys(eventSourcingProcessorMap[AggregateType]).forEach(EventType => {
            if (!acc[AggregateType][EventType]) { acc[AggregateType][EventType] = []; }
            acc[AggregateType][EventType].push(eventSourcingProcessorMap[AggregateType][EventType]);
          })
        })
        return acc;
      },
      {}
    );
  }

}

/**
 * @returns {EventStoreService}
 */
module.exports = () => {
  if (!instance) {
    instance = new EventStoreService();
    ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
  }
  return instance;
};

