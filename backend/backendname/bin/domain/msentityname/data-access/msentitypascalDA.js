"use strict";

let mongoDB = undefined;
const { map } = require("rxjs/operators");
const { of, Observable, defer } = require("rxjs");

const { CustomError } = require("@nebulae/backend-node-tools").error;

const CollectionName = 'msentitypascal';

class msentitypascalDA {
  static start$(mongoDbInstance) {
    return Observable.create(observer => {
      if (mongoDbInstance) {
        mongoDB = mongoDbInstance;
        observer.next(`${this.name} using given mongo instance`);
      } else {
        mongoDB = require("../../../tools/mongo-db/MongoDB").singleton();
        observer.next(`${this.name} using singleton system-wide mongo instance`);
      }
      observer.next(`${this.name} started`);
      observer.complete();
    });
  }

  /**
   * Gets an user by its username
   */
  static getmsentitypascal$(id) {
    const collection = mongoDB.db.collection(CollectionName);

    const query = {
      _id: id
    };
    return defer(() => collection.findOne(query));
  }

  static getmsentitypascalList$(filter, pagination) {
    const collection = mongoDB.db.collection(CollectionName);

    const query = {
    };

    if (filter.name) {
      query["generalInfo.name"] = { $regex: filter.name, $options: "i" };
    }

    if (filter.creationTimestamp) {
      query.creationTimestamp = filter.creationTimestamp;
    }

    if (filter.creatorUser) {
      query.creatorUser = { $regex: filter.creatorUser, $options: "i" };
    }

    if (filter.modifierUser) {
      query.modifierUser = { $regex: filter.modifierUser, $options: "i" };
    }

    const cursor = collection
      .find(query)
      .skip(pagination.count * pagination.page)
      .limit(pagination.count)
      .sort({ creationTimestamp: pagination.sort });

    return mongoDB.extractAllFromMongoCursor$(cursor);
  }

  static getmsentitypascalSize$(filter) {
    const collection = mongoDB.db.collection(CollectionName);

    const query = {
    };

    if (filter.name) {
      query["generalInfo.name"] = { $regex: filter.name, $options: "i" };
    }

    if (filter.creationTimestamp) {
      query.creationTimestamp = filter.creationTimestamp;
    }

    if (filter.creatorUser) {
      query.creatorUser = { $regex: filter.creatorUser, $options: "i" };
    }

    if (filter.modifierUser) {
      query.modifierUser = { $regex: filter.modifierUser, $options: "i" };
    }

    return defer(() => collection.count(query));
  }

  /**
   * Creates a new msentitypascal
   * @param {*} soapRequest soapRequest to create
   */
  static createmsentitypascal$(soapRequest) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() => collection.insertOne(soapRequest));
  }

  /**
* modifies the general info of the indicated msentitypascal 
* @param {*} id  msentitypascal ID
* @param {*} msentitypascalGeneralInfo  New general information of the msentitypascal
*/
  static updatemsentitypascalGeneralInfo$(_id, generalInfo, modifierUser, modificationTimestamp) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.findOneAndUpdate(
        { _id },
        {
          $set: { generalInfo, modifierUser, modificationTimestamp }
        }, {
          returnOriginal: false
        }
      )
    ).pipe(
      map(result => result && result.value ? result.value : undefined)
    );
  }

  /**
   * Updates the msentitypascal state 
   * @param {string} id msentitypascal ID
   * @param {boolean} newmsentitypascalState boolean that indicates the new msentitypascal state
   */
  static updatemsentitypascalState$(_id, state, modifierUser, modificationTimestamp) {
    const collection = mongoDB.db.collection(CollectionName);

    return defer(() =>
      collection.findOneAndUpdate(
        { _id },
        {
          $set: { state, modifierUser, modificationTimestamp }
        }, {
          returnOriginal: false
        }
      )
    ).pipe(
      map(result => result && result.value ? result.value : undefined)
    );
  }

}
/**
 * @returns {msentitypascalDA}
 */
module.exports = msentitypascalDA;
