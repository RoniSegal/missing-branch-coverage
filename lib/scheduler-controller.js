"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var cronParser = require("cron-parser");
var sl_cloud_infra2_1 = require("@sealights/sl-cloud-infra2");
var defaultTTLInSeconds = 60;
var SchedulerController = /** @class */ (function () {
    function SchedulerController(dbService, redisCacheService, coordinationService, eventCallbackNotifier, maxSchedulerRetries, logger) {
        this.dbService = dbService;
        this.redisCacheService = redisCacheService;
        this.coordinationService = coordinationService;
        this.eventCallbackNotifier = eventCallbackNotifier;
        this.maxSchedulerRetries = maxSchedulerRetries;
        this.logger = logger;
        this.stopped = false;
        if (!dbService) {
            throw new Error("dbServices is missing");
        }
        if (!redisCacheService) {
            throw new Error("redisCacheService is missing");
        }
        if (!coordinationService) {
            throw new Error("coordinationService is missing");
        }
        if (!eventCallbackNotifier) {
            throw new Error("publisher is missing");
        }
        if (maxSchedulerRetries == null) {
            throw new Error("maxSchedulerRetries is missing");
        }
        if (!logger) {
            throw new Error("logger is missing");
        }
        this.logger = logger.withClass("SchedulerController");
    }
    /**
     * Get future date by parsing the cron expression and getting the next date
     * Fallback to adding milliseconds to current datetime TTI
     * @param {IScheduledEvent} event
     * @param logger
     * @returns {Date}
     */
    SchedulerController.getFutureDate = function (event, logger) {
        if (event.cron) {
            var options = { utc: true };
            var startDate = SchedulerController.getStartDate(event.startDate, logger);
            if (startDate) {
                options.currentDate = startDate;
            }
            var cronExpression = cronParser.parseExpression(event.cron, options);
            if (event.everyXWeeks) {
                for (var num = 1; num <= event.everyXWeeks; num++) {
                    cronExpression.next();
                }
            }
            return cronExpression.next().toDate();
        }
        return new Date(Date.now() + event.TTI);
    };
    SchedulerController.getStartDate = function (stringStartDate, logger) {
        if (!stringStartDate) {
            return null;
        }
        var startDate = new Date(stringStartDate);
        if (isNaN(startDate.valueOf())) {
            logger.warn("startDate is not a valid date. using now");
            startDate = null;
        }
        return startDate;
    };
    SchedulerController.prototype.start = function (delay) {
        this.stopped = false;
        this.scheduleNextPoll(delay);
    };
    SchedulerController.prototype.stop = function () {
        this.stopped = true;
    };
    SchedulerController.prototype.isStopped = function () {
        return this.stopped;
    };
    SchedulerController.prototype.registerScheduledEvent = function (event, eventKey, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var now, jobTTI;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withClass("SchedulerController").withMethod("registerScheduledEvent");
                        now = new Date().valueOf();
                        return [4 /*yield*/, this.getJobTimestampFromCache(eventKey, logger)];
                    case 1:
                        jobTTI = _a.sent();
                        if (!jobTTI) return [3 /*break*/, 3];
                        // job already exists. in case of duplication we handle the earliest event
                        if (jobTTI > now && !event.resetTTI) {
                            logger.info("job with key: " + eventKey + " already exists, avoiding duplicated registration");
                            return [2 /*return*/];
                        }
                        // job already exists but not relevant - delete cached value
                        return [4 /*yield*/, this.invalidateCachedEvent(eventKey, logger)];
                    case 2:
                        // job already exists but not relevant - delete cached value
                        _a.sent();
                        _a.label = 3;
                    case 3: return [4 /*yield*/, this.addScheduledEvent(eventKey, event, true, logger)];
                    case 4:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.scheduleNextPoll = function (delay) {
        var _this = this;
        setTimeout(function () { return __awaiter(_this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                this.pollOnce().then(function () {
                    if (!_this.stopped) {
                        _this.scheduleNextPoll(delay);
                    }
                })["catch"](function () {
                    if (!_this.stopped) {
                        _this.scheduleNextPoll(delay);
                    }
                });
                return [2 /*return*/];
            });
        }); }, delay);
    };
    SchedulerController.prototype.pollOnce = function () {
        return __awaiter(this, void 0, void 0, function () {
            var logger, scheduledEvent, err_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = this.logger.withMethod("pollOnce");
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 5, , 6]);
                        return [4 /*yield*/, this.getNextScheduledEvent(logger)];
                    case 2:
                        scheduledEvent = _a.sent();
                        if (!scheduledEvent) return [3 /*break*/, 4];
                        return [4 /*yield*/, this.runJob(scheduledEvent)];
                    case 3:
                        _a.sent();
                        _a.label = 4;
                    case 4: return [3 /*break*/, 6];
                    case 5:
                        err_1 = _a.sent();
                        logger.error("Error in iteration", err_1);
                        return [3 /*break*/, 6];
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    /**
     * @param  {string} key
     * @param  {IScheduledEvent} event
     * @param  {boolean} upsert
     * @param  {ILogger} logger
     * @param  {string=key} lockKey is used when updating an existing job and we want to lock the old job key and not the new job key
     * @returns Promise
     */
    SchedulerController.prototype.addScheduledEvent = function (key, event, upsert, logger, lockKey) {
        if (lockKey === void 0) { lockKey = key; }
        return __awaiter(this, void 0, void 0, function () {
            var eventTimestamp, scheduledEventEntry, updatedEntry, err_2;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 5, 7, 9]);
                        eventTimestamp = SchedulerController.getFutureDate(event, logger);
                        scheduledEventEntry = this.createScheduledEventDbEntry(event, key, eventTimestamp);
                        return [4 /*yield*/, this.acquireLock(lockKey, logger)];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, this.upsertScheduledEventInDb(key, scheduledEventEntry, upsert, logger)];
                    case 2:
                        updatedEntry = _a.sent();
                        if (!updatedEntry) return [3 /*break*/, 4];
                        logger.info("scheduled event with key: " + key + " was added", updatedEntry);
                        return [4 /*yield*/, this.setJobTimestampInCache(key, this.getCacheTTL(event), updatedEntry.timestampTTI.valueOf(), logger)];
                    case 3:
                        _a.sent();
                        _a.label = 4;
                    case 4: return [2 /*return*/, updatedEntry];
                    case 5:
                        err_2 = _a.sent();
                        logger.error("Error adding scheduled event", err_2);
                        return [4 /*yield*/, this.invalidateCachedEvent(key, logger)];
                    case 6:
                        _a.sent();
                        throw (err_2);
                    case 7: return [4 /*yield*/, this.releaseLock(lockKey, logger)];
                    case 8:
                        _a.sent();
                        return [7 /*endfinally*/];
                    case 9: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.createScheduledEventDbEntry = function (event, key, timestamp) {
        var entry = event;
        entry.key = key;
        entry.timestampTTI = timestamp;
        return entry;
    };
    SchedulerController.prototype.runJob = function (event) {
        return __awaiter(this, void 0, void 0, function () {
            var logger, isPublished, err_3;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = this.logger.withMethod("runJob").withProperties(event).withTx(event.trackingId);
                        logger.info("running event with title : " + event.title, event);
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 6, 7, 10]);
                        return [4 /*yield*/, this.acquireLock(event.key, logger)];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, this.tryNotifyEndOfScheduledEvent(event, logger)];
                    case 3:
                        isPublished = _a.sent();
                        if (!isPublished) return [3 /*break*/, 5];
                        return [4 /*yield*/, this.finalizeScheduledEventTasks(event, logger)];
                    case 4:
                        _a.sent();
                        _a.label = 5;
                    case 5: return [3 /*break*/, 10];
                    case 6:
                        err_3 = _a.sent();
                        logger.error("Error handling scheduled event of " + event.title, err_3);
                        throw (err_3);
                    case 7:
                        if (!event.TTI) return [3 /*break*/, 9];
                        return [4 /*yield*/, this.releaseLock(event.key, logger)];
                    case 8:
                        _a.sent();
                        _a.label = 9;
                    case 9: return [7 /*endfinally*/];
                    case 10: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.tryNotifyEndOfScheduledEvent = function (event, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var err_4, err_5;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("tryNotifyEndOfScheduledEvent");
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 12]);
                        logger.info("notifying callbackQueue: " + event.callbackQueue + " about end of scheduled event", event);
                        return [4 /*yield*/, this.eventCallbackNotifier.notifyEndScheduledEvent(event, logger)];
                    case 2:
                        _a.sent();
                        return [2 /*return*/, true];
                    case 3:
                        err_4 = _a.sent();
                        if (!this.shouldRetry(event, logger)) return [3 /*break*/, 9];
                        _a.label = 4;
                    case 4:
                        _a.trys.push([4, 6, , 8]);
                        logger.warn("Notifying failed with error " + err_4.message + ", trying to retry scheduled event");
                        return [4 /*yield*/, this.retryScheduledEvent(event, logger)];
                    case 5:
                        _a.sent();
                        return [2 /*return*/, false];
                    case 6:
                        err_5 = _a.sent();
                        logger.error("Failed to retry scheduled event", err_5);
                        return [4 /*yield*/, this.removeScheduledEvent(event.key, logger)];
                    case 7:
                        _a.sent();
                        throw err_5;
                    case 8: return [3 /*break*/, 11];
                    case 9:
                        logger.error("Error notifying end of scheduled event, event will not be retried");
                        return [4 /*yield*/, this.removeScheduledEvent(event.key, logger)];
                    case 10:
                        _a.sent();
                        throw err_4;
                    case 11: return [3 /*break*/, 12];
                    case 12: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.acquireLock = function (key, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("acquireLock");
                        logger.info("acquiring lock with key : " + key);
                        return [4 /*yield*/, this.coordinationService.waitUntilLockAcquired(key, logger)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.releaseLock = function (key, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("releaseLock");
                        logger.info("releasing lock with key : " + key);
                        return [4 /*yield*/, this.coordinationService.unlockExistingLock(logger)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.getNextScheduledEvent = function (logger) {
        return __awaiter(this, void 0, void 0, function () {
            var scheduledEvent;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("getNextScheduledEvent");
                        logger.debug("getting next scheduled Event");
                        return [4 /*yield*/, this.dbService.getNextScheduledEvent(logger)];
                    case 1:
                        scheduledEvent = _a.sent();
                        return [2 /*return*/, scheduledEvent];
                }
            });
        });
    };
    SchedulerController.prototype.removeScheduledEvent = function (eventKey, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var err_6;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withClass("SchedulerController").withMethod("removeScheduledEvent");
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 4, , 5]);
                        return [4 /*yield*/, this.deleteScheduledEventFromDb(eventKey, logger)];
                    case 2:
                        _a.sent();
                        logger.info("deleted scheduledEvent with key : " + eventKey);
                        return [4 /*yield*/, this.invalidateCachedEvent(eventKey, logger)];
                    case 3:
                        _a.sent();
                        logger.info("invalidated scheduled event with key : " + eventKey);
                        return [3 /*break*/, 5];
                    case 4:
                        err_6 = _a.sent();
                        logger.error("Error removing scheduled event", err_6);
                        throw err_6;
                    case 5: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.updateScheduledEvent = function (updatedEventKey, event, oldEventKey, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var updatedEntry;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withClass("SchedulerController").withMethod("updateScheduledEvent");
                        return [4 /*yield*/, this.addScheduledEvent(updatedEventKey, event, false, logger, oldEventKey)];
                    case 1:
                        updatedEntry = _a.sent();
                        if (!updatedEntry) {
                            throw new sl_cloud_infra2_1.NotExistsError(oldEventKey);
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.deleteScheduledEventFromDb = function (key, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("deleteScheduledEventFromDb");
                        logger.info("deleting scheduledEvent with key : " + key);
                        return [4 /*yield*/, this.dbService.deleteScheduledEvent(key, logger)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    SchedulerController.prototype.upsertScheduledEventInDb = function (key, event, upsert, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("upsertScheduledEventInDb");
                        logger.info("upserting scheduledEvent with key : " + key + ", upsert: " + upsert + " data: " + JSON.stringify(event));
                        return [4 /*yield*/, this.dbService.updateScheduledEvent(key, event, upsert, logger)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    SchedulerController.prototype.setJobTimestampInCache = function (eventKey, secondsTTL, timestamp, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("setJobTimestampInCache");
                        logger.debug("setting scheduledEvent timestamp in cache with key : " + eventKey + ", value: " + timestamp);
                        return [4 /*yield*/, this.redisCacheService.setKey(eventKey, secondsTTL, timestamp.toString(), logger)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    SchedulerController.prototype.getJobTimestampFromCache = function (eventKey, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var value, err_7;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("getJobTimestampFromCache");
                        logger.debug("getting scheduledEvent timestamp from cache with key : " + eventKey);
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this.redisCacheService.getKey(eventKey, logger)];
                    case 2:
                        value = _a.sent();
                        if (!value) {
                            return [2 /*return*/, null];
                        }
                        return [2 /*return*/, JSON.parse(value)];
                    case 3:
                        err_7 = _a.sent();
                        logger.error("Error getting scheduled event's timestamp from cache", err_7);
                        throw err_7;
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.invalidateCachedEvent = function (eventKey, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("invalidateCachedEvent");
                        logger.info("invalidating scheduled event with key : " + eventKey);
                        return [4 /*yield*/, this.redisCacheService.deleteKey(eventKey, logger)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    SchedulerController.prototype.retryScheduledEvent = function (event, logger) {
        return __awaiter(this, void 0, void 0, function () {
            var updatedEntry, err_8;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        logger = logger.withMethod("retryScheduledEvent");
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this.upsertScheduledEventInDb(event.key, event, true, logger)];
                    case 2:
                        updatedEntry = _a.sent();
                        if (updatedEntry) {
                            logger.info("event with key: " + event.key + " will be retried later, retry count of " + event.payload.retryCount);
                        }
                        return [3 /*break*/, 4];
                    case 3:
                        err_8 = _a.sent();
                        logger.error("Error in updating retry count in db", err_8);
                        throw err_8;
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    SchedulerController.prototype.shouldRetry = function (event, logger) {
        var shouldRetry = true;
        event.retryCount = event.retryCount ? ++event.payload.retryCount : 1;
        if (event.retryCount > this.maxSchedulerRetries) {
            logger.warn("event retries exceeded max retries allowed, (" + this.maxSchedulerRetries + " retries. scheduled event will not be retied)", event);
            shouldRetry = false;
        }
        return shouldRetry;
    };
    SchedulerController.prototype.getCacheTTL = function (event) {
        if (event.TTI) {
            return event.TTI / 1000;
        }
        return defaultTTLInSeconds;
    };
    SchedulerController.prototype.finalizeScheduledEventTasks = function (event, logger) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!event.TTI) return [3 /*break*/, 2];
                        return [4 /*yield*/, this.removeScheduledEvent(event.key, logger)];
                    case 1: return [2 /*return*/, _a.sent()];
                    case 2: return [4 /*yield*/, this.releaseLock(event.key, logger)];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, this.addScheduledEvent(event.key, event, true, logger)];
                    case 4:
                        _a.sent();
                        _a.label = 5;
                    case 5: return [2 /*return*/];
                }
            });
        });
    };
    return SchedulerController;
}());
exports.SchedulerController = SchedulerController;
//# sourceMappingURL=scheduler-controller.js.map