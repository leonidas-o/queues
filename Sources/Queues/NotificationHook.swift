import NIO

/// Represents an object that can receive notifications about job statuses
@available(macOS 12.0.0, *)
public protocol JobEventDelegate {

    /// Called when the job is first dispatched
    /// - Parameters:
    ///   - job: The `JobData` associated with the job
    func dispatched(job: JobEventData) async throws -> Void

    /// Called when the job is dequeued
    /// - Parameters:
    ///   - jobId: The id of the Job
    func didDequeue(jobId: String) async throws -> Void


    /// Called when the job succeeds
    /// - Parameters:
    ///   - jobId: The id of the Job
    func success(jobId: String) async throws -> Void

    /// Called when the job returns an error
    /// - Parameters:
    ///   - jobId: The id of the Job
    ///   - error: The error that caused the job to fail
    func error(jobId: String, error: Error) async throws -> Void
}

@available(macOS 12.0.0, *)
extension JobEventDelegate {
    public func dispatched(job: JobEventData) async throws -> Void {
        return
    }

    public func didDequeue(jobId: String) async throws -> Void {
        return
    }

    public func success(jobId: String) async throws -> Void {
        return
    }

    public func error(jobId: String, error: Error) async throws -> Void {
        return
    }
}

/// Data on a job sent via a notification
public struct JobEventData {
    /// The id of the job, assigned at dispatch
    public var id: String

    /// The name of the queue (i.e. `default`)
    public var queueName: String

    /// The job data to be encoded.
    public var payload: [UInt8]

    /// The maxRetryCount for the `Job`.
    public var maxRetryCount: Int

    /// A date to execute this job after
    public var delayUntil: Date?

    /// The date this job was queued
    public var queuedAt: Date

    /// The name of the `Job`
    public var jobName: String

    /// Creates a new `JobStorage` holding object
    public init(id: String, queueName: String, jobData: JobData) {
        self.id = id
        self.queueName = queueName
        self.payload = jobData.payload
        self.maxRetryCount = jobData.maxRetryCount
        self.jobName = jobData.jobName
        self.delayUntil = jobData.delayUntil
        self.queuedAt = jobData.queuedAt
    }
}
