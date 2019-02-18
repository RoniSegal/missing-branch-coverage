export class SchedulerController {

    public async pollOnce(param: number): Promise<boolean> {
        try {
            const scheduledEvent = await this.getEventTitle(param);
            if (scheduledEvent) {
                await this.runJob(scheduledEvent);
                return true;
            }
            return false;
        } catch (err) {
            console.error("Error in iteration", err);
            return false;
        }
    }

    public async runJob(event: string) {
        return new Promise((resolve, reject) => {
            if (event == "failed") {
                reject("Event should fail")
            }
            resolve();
        })
    }

    public async getEventTitle(param: number): Promise<string> {
        return new Promise((resolve, reject) => {
            if (param === 0) {
                resolve(null)
            }
            if (param > 10) {
                reject("Param should be < 10 ")
            }
            if (param > 0) {
                resolve("Success event")
            } else {
                resolve("failed");
            }
        })
    }

}
