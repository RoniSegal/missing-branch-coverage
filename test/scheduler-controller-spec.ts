import {sandbox} from "sinon";
import {expect} from "chai"
import {SchedulerController} from "../lib/scheduler-controller";

let mocks: any;

describe("SchedulerController", () => {
    beforeEach(() => {
        mocks = sandbox.create();
    });
    afterEach(() => {
        mocks.restore();
    });

    it("should return false", async () => {
        const scheduler = new SchedulerController();
        const result = await scheduler.pollOnce(-1)
        expect(result).false;
    });
    it("should return false event title null", async () => {
        const scheduler = new SchedulerController();
        const result = await scheduler.pollOnce(0)
        expect(result).false;
    });
    it("should return true", async () => {
        const scheduler = new SchedulerController();
        const result = await scheduler.pollOnce(1)
        expect(result).true;
    });

    it("should return false - throws ", async () => {
        const scheduler = new SchedulerController();
        const result = await scheduler.pollOnce(11)
        expect(result).false;
    });
});