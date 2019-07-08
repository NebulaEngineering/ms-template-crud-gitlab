"use strict";

const Rx = require('rxjs');

const msentitypascalDA = require("./msentitypascalDA");

module.exports = {
  /**
   * Data-Access start workflow
   */
  start$: Rx.concat(msentitypascalDA.start$()),
  /**
   * @returns {msentitypascalDA}
   */
  msentitypascalDA,
};
