/// A specific queue that jobs are run on.
public struct QueueName {
    /// The default queue that jobs are run on
    public static let `default` = QueueName(string: "default")

    /// The name of the queue
    public let string: String
    
    /// If the queue is executing the jobs sequentially
    public let isSequential: Bool

    /// Creates a new `QueueType`
    ///
    /// - Parameters:
    ///   - name: The name of the `QueueType`
    ///   - isSequential: If the queue is executing the jobs sequentially
    public init(string: String, isSequential: Bool = false) {
        self.string = string
        self.isSequential = isSequential
    }

    /// Makes the name of the queue
    ///
    /// - Parameter persistanceKey: The base persistence key
    /// - Returns: A string of the queue's fully qualified name
    public func makeKey(with persistanceKey: String) -> String {
        return persistanceKey + "[\(self.string)]"
    }
}
