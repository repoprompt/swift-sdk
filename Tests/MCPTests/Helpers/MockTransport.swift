import Logging

import struct Foundation.Data
import class Foundation.JSONDecoder
import class Foundation.JSONEncoder
import protocol Foundation.LocalizedError
import struct Foundation.POSIXError

@testable import MCP

/// Mock transport for testing
actor MockTransport: Transport {
    enum ResponseSendFaultBehavior: Sendable {
        case throwError(message: String = "Injected response send failure")
        case suspend
    }

    private struct ResponseSendFaultError: LocalizedError, Sendable {
        let message: String
        var errorDescription: String? { message }
    }

    struct ResponseSendFaultSelector: Sendable {
        let id: ID
        let method: String?
        let toolName: String?
        let behavior: ResponseSendFaultBehavior

        init(
            id: ID,
            method: String? = nil,
            toolName: String? = nil,
            behavior: ResponseSendFaultBehavior
        ) {
            self.id = id
            self.method = method
            self.toolName = toolName
            self.behavior = behavior
        }
    }

    private struct RequestMetadata: Sendable {
        let method: String
        let toolName: String?
    }

    var logger: Logger

    let encoder = JSONEncoder()
    let decoder = JSONDecoder()

    var isConnected = false
    private(set) var disconnectCallCount = 0

    private(set) var sentData: [Data] = []
    private(set) var sendAttempts = 0
    private(set) var responseSendAttempts: [ID: Int] = [:]
    var sentMessages: [String] {
        return sentData.compactMap { data in
            guard let string = String(data: data, encoding: .utf8) else {
                logger.error("Failed to decode sent data as UTF-8")
                return nil
            }
            return string
        }
    }

    private var disconnectionObservers: [AsyncStream<Void>.Continuation] = []

    private var pairedTransport: MockTransport?
    private var dataToReceive: [Data] = []
    private(set) var receivedMessages: [String] = []

    private var dataStreamContinuation: AsyncThrowingStream<Data, Swift.Error>.Continuation?
    private var shouldFinishOnReceive = false
    private var receiveErrorOnReceive: MCPError?

    private var requestMetadataByID: [ID: RequestMetadata] = [:]
    private var responseSendFault: ResponseSendFaultSelector?
    private var observedResponseSendIDs: Set<ID> = []
    private var responseSendStartWaiters: [ID: [CheckedContinuation<Void, Never>]] = [:]
    private var suspendedSendContinuations: [ID: [CheckedContinuation<Void, Swift.Error>]] = [:]

    var shouldFailConnect = false
    var shouldFailSend = false
    var failOnSendAttempt: Int?

    init(logger: Logger = Logger(label: "mcp.test.transport")) {
        self.logger = logger
    }

    static func createConnectedPair(
        logger: Logger = Logger(label: "mcp.test.transport")
    ) async -> (client: MockTransport, server: MockTransport) {
        let client = MockTransport(logger: logger)
        let server = MockTransport(logger: logger)
        await client.pair(with: server)
        await server.pair(with: client)
        return (client, server)
    }

    private func pair(with transport: MockTransport) {
        pairedTransport = transport
    }

    public func connect() async throws {
        if shouldFailConnect {
            throw MCPError.transportError(POSIXError(.ECONNREFUSED))
        }
        isConnected = true
    }

    func disconnectionEvents() -> AsyncStream<Void> {
        let (stream, continuation) = AsyncStream<Void>.makeStream(
            bufferingPolicy: .bufferingNewest(1))
        if isConnected {
            disconnectionObservers.append(continuation)
        } else {
            continuation.yield(())
            continuation.finish()
        }
        return stream
    }

    private func publishDisconnection() {
        for observer in disconnectionObservers {
            observer.yield(())
            observer.finish()
        }
        disconnectionObservers.removeAll()
    }

    public func disconnect() async {
        disconnectCallCount += 1
        guard isConnected else { return }

        isConnected = false
        publishDisconnection()
        dataStreamContinuation?.finish()
        dataStreamContinuation = nil
        resumeSuspendedSends(throwing: MCPError.connectionClosed)

        if let pairedTransport {
            await pairedTransport.handlePeerDisconnection()
        }
    }

    private func handlePeerDisconnection() {
        guard isConnected else { return }

        isConnected = false
        publishDisconnection()
        dataStreamContinuation?.finish(throwing: MCPError.connectionClosed)
        dataStreamContinuation = nil
        resumeSuspendedSends(throwing: MCPError.connectionClosed)
    }

    public func send(_ message: Data) async throws {
        sendAttempts += 1
        if shouldFailSend || failOnSendAttempt == sendAttempts {
            throw MCPError.transportError(POSIXError(.EIO))
        }

        if let (responseID, fault) = matchingResponseFault(in: message) {
            responseSendAttempts[responseID, default: 0] += 1
            markResponseSendStarted(id: responseID)

            switch fault.behavior {
            case .throwError(let message):
                throw MCPError.transportError(ResponseSendFaultError(message: message))
            case .suspend:
                try await withCheckedThrowingContinuation { continuation in
                    suspendedSendContinuations[responseID, default: []].append(continuation)
                }
            }
        } else {
            recordResponseSendAttempts(in: message)
        }

        guard isConnected else {
            throw MCPError.connectionClosed
        }

        sentData.append(message)
        if let pairedTransport {
            await pairedTransport.deliver(message)
        }
    }

    public func receive() -> AsyncThrowingStream<Data, Swift.Error> {
        return AsyncThrowingStream<Data, Swift.Error> { continuation in
            dataStreamContinuation = continuation
            for message in dataToReceive {
                continuation.yield(message)
                if let string = String(data: message, encoding: .utf8) {
                    receivedMessages.append(string)
                }
            }
            dataToReceive.removeAll()

            if let receiveErrorOnReceive {
                continuation.finish(throwing: receiveErrorOnReceive)
                self.receiveErrorOnReceive = nil
                dataStreamContinuation = nil
            } else if shouldFinishOnReceive {
                continuation.finish()
                shouldFinishOnReceive = false
                dataStreamContinuation = nil
            }
        }
    }

    func setFailConnect(_ shouldFail: Bool) {
        shouldFailConnect = shouldFail
    }

    func setFailSend(_ shouldFail: Bool) {
        shouldFailSend = shouldFail
    }

    func setFailOnSendAttempt(_ attempt: Int?) {
        failOnSendAttempt = attempt
    }

    func setResponseSendFault(_ fault: ResponseSendFaultSelector?) {
        responseSendFault = fault
    }

    func waitUntilResponseSendStarted(id: ID) async {
        if observedResponseSendIDs.contains(id) {
            return
        }
        await withCheckedContinuation { continuation in
            responseSendStartWaiters[id, default: []].append(continuation)
        }
    }

    func responseSendAttemptCount(for id: ID) -> Int {
        responseSendAttempts[id, default: 0]
    }

    func finishReceiving() {
        if let continuation = dataStreamContinuation {
            continuation.finish()
            dataStreamContinuation = nil
        } else {
            shouldFinishOnReceive = true
        }
    }

    func failReceiving(with error: MCPError = MCPError.transportError(POSIXError(.EIO))) {
        if let continuation = dataStreamContinuation {
            continuation.finish(throwing: error)
            dataStreamContinuation = nil
        } else {
            receiveErrorOnReceive = error
        }
    }

    func queue(data: Data) {
        recordRequestMetadata(in: data)
        if let continuation = dataStreamContinuation {
            continuation.yield(data)
        } else {
            dataToReceive.append(data)
        }
    }

    func queue<M: Method>(request: Request<M>) throws {
        queue(data: try encoder.encode(request))
    }

    func queue<M: Method>(response: Response<M>) throws {
        queue(data: try encoder.encode(response))
    }

    func queue<N: Notification>(notification: Message<N>) throws {
        queue(data: try encoder.encode(notification))
    }

    func queue(batch requests: [AnyRequest]) throws {
        queue(data: try encoder.encode(requests))
    }

    func queue(batch responses: [AnyResponse]) throws {
        queue(data: try encoder.encode(responses))
    }

    func decodeLastSentMessage<T: Decodable>() -> T? {
        guard let lastMessage = sentData.last else { return nil }
        do {
            return try decoder.decode(T.self, from: lastMessage)
        } catch {
            return nil
        }
    }

    func clearMessages() {
        sentData.removeAll()
        sendAttempts = 0
        responseSendAttempts.removeAll()
        observedResponseSendIDs.removeAll()
        dataToReceive.removeAll()
    }

    private func deliver(_ data: Data) {
        recordRequestMetadata(in: data)
        if let continuation = dataStreamContinuation {
            continuation.yield(data)
        } else {
            dataToReceive.append(data)
        }
    }

    private func recordRequestMetadata(in data: Data) {
        if let request = try? decoder.decode(AnyRequest.self, from: data) {
            recordRequestMetadata(request)
        } else if let requests = try? decoder.decode([AnyRequest].self, from: data) {
            for request in requests {
                recordRequestMetadata(request)
            }
        }
    }

    private func recordRequestMetadata(_ request: AnyRequest) {
        let toolName: String?
        if request.method == CallTool.name,
            case .object(let parameters) = request.params
        {
            toolName = parameters["name"]?.stringValue
        } else {
            toolName = nil
        }
        requestMetadataByID[request.id] = RequestMetadata(
            method: request.method,
            toolName: toolName
        )
    }

    private func matchingResponseFault(
        in data: Data
    ) -> (ID, ResponseSendFaultSelector)? {
        guard let responseSendFault else { return nil }

        if let response = try? decoder.decode(AnyResponse.self, from: data),
            responseMatchesFault(response, fault: responseSendFault)
        {
            return (response.id, responseSendFault)
        }

        if let responses = try? decoder.decode([AnyResponse].self, from: data),
            let response = responses.first(where: {
                responseMatchesFault($0, fault: responseSendFault)
            })
        {
            return (response.id, responseSendFault)
        }

        return nil
    }

    private func responseMatchesFault(
        _ response: AnyResponse,
        fault: ResponseSendFaultSelector
    ) -> Bool {
        guard response.id == fault.id else { return false }
        let metadata = requestMetadataByID[response.id]
        if let method = fault.method, metadata?.method != method {
            return false
        }
        if let toolName = fault.toolName, metadata?.toolName != toolName {
            return false
        }
        return true
    }

    private func recordResponseSendAttempts(in data: Data) {
        if let response = try? decoder.decode(AnyResponse.self, from: data) {
            responseSendAttempts[response.id, default: 0] += 1
        } else if let responses = try? decoder.decode([AnyResponse].self, from: data) {
            for response in responses {
                responseSendAttempts[response.id, default: 0] += 1
            }
        }
    }

    private func markResponseSendStarted(id: ID) {
        observedResponseSendIDs.insert(id)
        let waiters = responseSendStartWaiters.removeValue(forKey: id) ?? []
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func resumeSuspendedSends(throwing error: Swift.Error) {
        let continuations = suspendedSendContinuations
        suspendedSendContinuations.removeAll()
        for continuation in continuations.values.flatMap({ $0 }) {
            continuation.resume(throwing: error)
        }
    }
}
