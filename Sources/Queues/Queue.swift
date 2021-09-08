/// A type that can store and retrieve jobs from a persistence layer
@available(macOS 12.0.0, *)
public protocol Queue {
    /// The job context
    var context: QueueContext { get }
    
    /// Gets the next job to be run
    /// - Parameter id: The ID of the job
    func get(_ id: JobIdentifier) async throws -> JobData
    
    /// Sets a job that should be run in the future
    /// - Parameters:
    ///   - id: The ID of the job
    ///   - data: Data for the job
    func set(_ id: JobIdentifier, to data: JobData) async throws -> Void
    
    /// Removes a job from the queue
    /// - Parameter id: The ID of the job
    func clear(_ id: JobIdentifier) async throws -> Void

    /// Pops the next job in the queue
    func pop() async throws -> JobIdentifier?
    
    /// Pushes the next job into a queue
    /// - Parameter id: The ID of the job
    func push(_ id: JobIdentifier) async throws -> Void
}

@available(macOS 12.0.0, *)
extension Queue {
    /// The EventLoop for a job queue
    public var eventLoop: EventLoop {
        self.context.eventLoop
    }
    
    /// A logger
    public var logger: Logger {
        self.context.logger
    }
    
    /// The configuration for the queue
    public var configuration: QueuesConfiguration {
        self.context.configuration
    }
    
    /// The queue's name
    public var queueName: QueueName {
        self.context.queueName
    }
    
    /// The key name of the queue
    public var key: String {
        self.queueName.makeKey(with: self.configuration.persistenceKey)
    }
    
    /// Dispatch a job into the queue for processing
    /// - Parameters:
    ///   - job: The Job type
    ///   - payload: The payload data to be dispatched
    ///   - maxRetryCount: Number of times to retry this job on failure
    ///   - delayUntil: Delay the processing of this job until a certain date
    public func dispatch<J>(
        _ job: J.Type,
        _ payload: J.Payload,
        maxRetryCount: Int = 0,
        delayUntil: Date? = nil,
        id: JobIdentifier = JobIdentifier()
    ) async throws -> Void
        where J: Job
    {
        let bytes = try J.serializePayload(payload)
        logger.trace("Serialized bytes for payload: \(bytes)")
        let storage = JobData(
            payload: bytes,
            maxRetryCount: maxRetryCount,
            jobName: J.name,
            delayUntil: delayUntil,
            queuedAt: Date()
        )
        logger.trace("Adding the ID to the storage")
        try await self.set(id, to: storage)
        try await self.push(id)
        self.logger.info("Dispatched queue job", metadata: [
            "job_id": .string(id.string),
            "job_name": .string(job.name),
            "queue": .string(self.queueName.string)
        ])
        
        return self
            .configuration
            .dispatchNotifications(job: .init(id: id.string,
                                              queueName: self.queueName.string,
                                              jobData: storage),
                                   id: id.string,
                                   type: .dispatched)
    }
}
