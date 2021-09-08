@available(macOS 12.0.0, *)
extension Queue {
    public var worker: QueueWorker {
        .init(queue: self)
    }
}

/// The worker that runs the `Job`
@available(macOS 12.0.0, *)
public struct QueueWorker {
    let queue: Queue

    init(queue: Queue) {
        self.queue = queue
    }
    
    /// Logic to run the queue
    public func run() async throws -> Void {
        queue.logger.trace("Popping job from queue")
        guard let id = try await self.queue.pop() else {
            //No job found, go to the next iteration
            self.queue.logger.trace("Did not receive ID from pop")
            return
        }
        
        var logger = self.queue.logger
        logger[metadataKey: "job_id"] = .string(id.string)
        
        let data = try await self.queue.get(id)
        
        logger.trace("Received job data for \(id): \(data)")
        logger.trace("Received job \(id)")
        logger.trace("Getting data for job \(id)")
        
        // If the job has a delay, we must check to make sure we can execute.
        // If the delay has not passed yet, requeue the job
        if let delay = data.delayUntil, delay >= Date() {
            logger.trace("Requeing job \(id) for execution later because the delayUntil value of \(delay) has not passed yet")
            return try await self.queue.push(id)
        }

        guard let job = self.queue.configuration.jobs[data.jobName] else {
            logger.error("No job named \(data.jobName) is registered")
            return
        }
        
        logger.trace("Sending dequeued notification hooks")
        
        self.queue.configuration.dispatchNotifications(id: id.string, type: .didDequeue)
        
        logger.info("Dequeing job", metadata: [
            "job_id": .string(id.string),
            "job_name": .string(data.jobName),
            "queue": .string(self.queue.queueName.string)
        ])

        return try await self.run(
            id: id,
            name: data.jobName,
            job: job,
            payload: data.payload,
            logger: logger,
            remainingTries: data.maxRetryCount,
            attempts: data.attempts,
            jobData: data
        )
    }

    private func run(
        id: JobIdentifier,
        name: String,
        job: AnyJob,
        payload: [UInt8],
        logger: Logger,
        remainingTries: Int,
        attempts: Int?,
        jobData: JobData
    ) async throws -> Void {
        logger.trace("Running the queue job (remaining tries: \(remainingTries)")
        do {
            try await job._dequeue(self.queue.context, id: id.string, payload: payload)
            logger.trace("Ran job successfully")
            logger.trace("Sending success notification hooks")
            self.queue.configuration.dispatchNotifications(id: id.string, type: .success)
            logger.trace("Job done being run")
            return try await self.queue.clear(id)
        } catch {
            logger.trace("Job failed (remaining tries: \(remainingTries))")
            if remainingTries == 0 {
                logger.error("Job failed with error: \(error)", metadata: [
                    "job_id": .string(id.string),
                    "job_name": .string(name),
                    "queue": .string(self.queue.queueName.string)
                ])

                logger.trace("Sending failure notification hooks")
                try await job._error(self.queue.context, id: id.string, error, payload: payload)
                self.queue.configuration.dispatchNotifications(id: id.string, error: error, type: .error)
                logger.trace("Job done being run")
                return try await self.queue.clear(id)
            } else {
                return try await self.retry(
                    id: id,
                    name: name,
                    job: job,
                    payload: payload,
                    logger: logger,
                    remainingTries: remainingTries,
                    attempts: attempts,
                    jobData: jobData,
                    error: error
                )
            }
        }
    }

    private func retry(
        id: JobIdentifier,
        name: String,
        job: AnyJob,
        payload: [UInt8],
        logger: Logger,
        remainingTries: Int,
        attempts: Int?,
        jobData: JobData,
        error: Error
    ) async throws -> Void {
        let attempts = attempts ?? 0
        let delayInSeconds = job._nextRetryIn(attempt: attempts + 1)
        if delayInSeconds == -1 {
            logger.error("Job failed, retrying... \(error)", metadata: [
                "job_id": .string(id.string),
                "job_name": .string(name),
                "queue": .string(self.queue.queueName.string)
            ])
            return try await self.run(
                id: id,
                name: name,
                job: job,
                payload: payload,
                logger: logger,
                remainingTries: remainingTries - 1,
                attempts: attempts + 1,
                jobData: jobData
            )
        } else {
            logger.error("Job failed, retrying in \(delayInSeconds)s... \(error)", metadata: [
                "job_id": .string(id.string),
                "job_name": .string(name),
                "queue": .string(self.queue.queueName.string)
            ])
            let storage = JobData(
                payload: jobData.payload,
                maxRetryCount: remainingTries - 1,
                jobName: jobData.jobName,
                delayUntil: Date(timeIntervalSinceNow: Double(delayInSeconds)),
                queuedAt: jobData.queuedAt,
                attempts: attempts + 1
            )
            
            try await self.queue.clear(id)
            try await self.queue.set(id, to: storage)
            try await self.queue.push(id)
        }
    }
}
