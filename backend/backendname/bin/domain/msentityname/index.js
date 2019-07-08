"use strict";

const { empty, Observable } = require("rxjs");

const msentitypascalCQRS = require("./msentitypascalCQRS")();
const msentitypascalES = require("./msentitypascalES")();
const DataAcess = require("./data-access/");

module.exports = {
  /**
   * domain start workflow
   */
  start$: DataAcess.start$,
  /**
   * start for syncing workflow
   * @returns {Observable}
   */
  startForSyncing$: DataAcess.start$,
  /**
   * start for getting ready workflow
   * @returns {Observable}
   */
  startForGettingReady$: empty(),
  /**
   * Stop workflow
   * @returns {Observable}
   */
  stop$: empty(),
  /**
   * @returns {msentitypascalCQRS}
   */
  msentitypascalCQRS,
  /**
   * @returns {msentitypascalES}
   */
  msentitypascalES,
  /**
   * EventSoircing event processors Map
   */
  eventSourcingProcessorMap: msentitypascalES.generateEventProcessorMap(),
  /**
   * CQRS request processors Map
   */
  cqrsRequestProcessorMap: msentitypascalCQRS.generateRequestProcessorMap(),
};
