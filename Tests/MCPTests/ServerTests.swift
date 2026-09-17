import Foundation
import Logging
import Testing

@testable import MCP

private actor ResponseDeliveryRequestGate {
    private var arrived: Set<ID> = []
    private var arrivalWaiters: [(Set<ID>, CheckedContinuation<Void, Never>)] = []
    private var released: Set<ID> = []
    private var releaseWaiters: [ID: [Int: CheckedContinuation<Void, Swift.Error>]] = [:]
    private var nextToken = 0

    func enter(id: ID) async throws {
        arrived.insert(id)
        resumeSatisfiedArrivalWaiters()
        if released.contains(id) { return }

        let token = nextToken
        nextToken += 1
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Swift.Error>) in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                } else if released.contains(id) {
                    continuation.resume()
                } else {
                    releaseWaiters[id, default: [:]][token] = continuation
                }
            }
        } onCancel: {
            Task { await self.cancel(id: id, token: token) }
        }
    }

    func waitUntilArrived(_ ids: Set<ID>) async {
        if ids.isSubset(of: arrived) { return }
        await withCheckedContinuation { continuation in
            arrivalWaiters.append((ids, continuation))
        }
    }

    func release(id: ID) {
        released.insert(id)
        let waiters = releaseWaiters.removeValue(forKey: id) ?? [:]
        for waiter in waiters.values {
            waiter.resume()
        }
    }

    func hasArrived(_ id: ID) -> Bool {
        arrived.contains(id)
    }

    private func cancel(id: ID, token: Int) {
        guard let waiter = releaseWaiters[id]?.removeValue(forKey: token) else { return }
        if releaseWaiters[id]?.isEmpty == true {
            releaseWaiters[id] = nil
        }
        waiter.resume(throwing: CancellationError())
    }

    private func resumeSatisfiedArrivalWaiters() {
        var remaining: [(Set<ID>, CheckedContinuation<Void, Never>)] = []
        for (ids, waiter) in arrivalWaiters {
            if ids.isSubset(of: arrived) {
                waiter.resume()
            } else {
                remaining.append((ids, waiter))
            }
        }
        arrivalWaiters = remaining
    }
}

private actor ManualResponseSendDeadline {
    private var sleepers: [Int: CheckedContinuation<Void, Swift.Error>] = [:]
    private var armedWaiters: [CheckedContinuation<Void, Never>] = []
    private var nextToken = 0

    func sleep(for _: Duration) async throws {
        let token = nextToken
        nextToken += 1
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Swift.Error>) in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                } else {
                    sleepers[token] = continuation
                    let waiters = armedWaiters
                    armedWaiters = []
                    for waiter in waiters {
                        waiter.resume()
                    }
                }
            }
        } onCancel: {
            Task { await self.cancel(token: token) }
        }
    }

    func waitUntilArmed() async {
        if !sleepers.isEmpty { return }
        await withCheckedContinuation { continuation in
            armedWaiters.append(continuation)
        }
    }

    func expire() {
        let sleepers = sleepers
        self.sleepers = [:]
        for sleeper in sleepers.values {
            sleeper.resume()
        }
    }

    private func cancel(token: Int) {
        sleepers.removeValue(forKey: token)?.resume(throwing: CancellationError())
    }
}

private enum ClientWaitOutcome: Equatable, Sendable {
    case success
    case mcpError(MCPError)
    case cancelled
    case other(String)
}

private func observeClientWaiter(
    _ context: RequestContext<CallTool.Result>
) async -> ClientWaitOutcome {
    do {
        _ = try await context.value
        return .success
    } catch let error as MCPError {
        return .mcpError(error)
    } catch is CancellationError {
        return .cancelled
    } catch {
        return .other(String(describing: error))
    }
}

private final class ServerTestLogRecorder: @unchecked Sendable {
    struct Entry: Sendable {
        let message: String
        let metadata: [String: String]
    }

    private let lock = NSLock()
    private var storedEntries: [Entry] = []

    func record(message: String, metadata: Logger.Metadata) {
        lock.lock()
        storedEntries.append(
            Entry(
                message: message,
                metadata: metadata.mapValues { String(describing: $0) }
            )
        )
        lock.unlock()
    }

    func entries() -> [Entry] {
        lock.lock()
        defer { lock.unlock() }
        return storedEntries
    }
}

private struct ServerTestLogHandler: LogHandler {
    var metadata: Logger.Metadata = [:]
    var logLevel: Logger.Level = .trace
    let recorder: ServerTestLogRecorder

    subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }

    func log(
        level _: Logger.Level,
        message: Logger.Message,
        metadata explicitMetadata: Logger.Metadata?,
        source _: String,
        file _: String,
        function _: String,
        line _: UInt
    ) {
        var combinedMetadata = metadata
        if let explicitMetadata {
            combinedMetadata.merge(explicitMetadata, uniquingKeysWith: { _, new in new })
        }
        recorder.record(message: String(describing: message), metadata: combinedMetadata)
    }
}

private struct SlowServerTestMethod: MCP.Method {
    static let name = "test/slow"
    typealias Parameters = Empty
    typealias Result = Empty
}

@Suite("Server Tests")
struct ServerTests {
    @Test("Start and stop server")
    func testServerStartAndStop() async throws {
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")

        #expect(await transport.isConnected == false)
        try await server.start(transport: transport)
        #expect(await transport.isConnected == true)
        await server.stop()
        #expect(await transport.isConnected == false)
    }

    @Test("Initialize request handling")
    func testServerHandleInitialize() async throws {
        let transport = MockTransport()

        // Queue an initialize request
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "TestClient", version: "1.0")
                )
            ))

        // Start the server
        let server: Server = Server(
            name: "TestServer",
            version: "1.0"
        )
        try await server.start(transport: transport)

        // Wait for message processing and response
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sentMessages.count == 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("serverInfo"))
        }

        // Clean up
        await server.stop()
        await transport.disconnect()
    }

    @Test("Initialize decodes object-valued experimental capabilities")
    func testInitializeDecodesObjectValuedExperimentalCapabilities() throws {
        let requestJSON = #"""
            {
              "jsonrpc": "2.0",
              "id": 0,
              "method": "initialize",
              "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {
                  "experimental": {
                    "codex/auth-change": {}
                  },
                  "elicitation": {
                    "form": {},
                    "url": {}
                  }
                },
                "clientInfo": {
                  "name": "codex-mcp-client",
                  "title": "Codex",
                  "version": "0.154.0"
                }
              }
            }
            """#

        let request = try JSONDecoder().decode(
            Request<Initialize>.self,
            from: Data(requestJSON.utf8)
        )

        #expect(
            request.params.capabilities.experimental == [
                "codex/auth-change": .object([:])
            ]
        )
    }

    @Test("Initialize preserves unknown experimental JSON values")
    func testInitializePreservesUnknownExperimentalJSONValues() throws {
        let capabilitiesJSON = #"""
            {
              "experimental": {
                "example/metadata": {
                  "enabled": true,
                  "modes": [1, "two"]
                }
              }
            }
            """#

        let capabilities = try JSONDecoder().decode(
            Client.Capabilities.self,
            from: Data(capabilitiesJSON.utf8)
        )

        #expect(
            capabilities.experimental == [
                "example/metadata": .object([
                    "enabled": .bool(true),
                    "modes": .array([.int(1), .string("two")]),
                ])
            ]
        )

        let roundTripped = try JSONDecoder().decode(
            Client.Capabilities.self,
            from: JSONEncoder().encode(capabilities)
        )
        #expect(roundTripped == capabilities)
    }

    @Test("Initialize distinguishes empty and absent experimental capabilities")
    func testInitializeDistinguishesEmptyAndAbsentExperimentalCapabilities() throws {
        let empty = try JSONDecoder().decode(
            Client.Capabilities.self,
            from: Data(#"{"experimental":{}}"#.utf8)
        )
        let legacy = try JSONDecoder().decode(
            Client.Capabilities.self,
            from: Data(#"{}"#.utf8)
        )

        #expect(empty.experimental == [:])
        #expect(legacy.experimental == nil)
    }

    @Test("Initialize hook - successful")
    func testInitializeHookSuccess() async throws {
        let transport = MockTransport()

        actor TestState {
            var hookCalled = false
            func setHookCalled() { hookCalled = true }
            func wasHookCalled() -> Bool { hookCalled }
        }

        let state = TestState()
        let server = Server(name: "TestServer", version: "1.0")

        // Start with the hook directly
        try await server.start(transport: transport) { clientInfo, capabilities in
            #expect(clientInfo.name == "TestClient")
            #expect(clientInfo.version == "1.0")
            await state.setHookCalled()
        }

        // Wait for server to initialize
        try await Task.sleep(for: .milliseconds(10))

        // Queue an initialize request
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "TestClient", version: "1.0")
                )
            ))

        // Wait for message processing and hook execution
        try await Task.sleep(for: .milliseconds(500))

        #expect(await state.wasHookCalled() == true)
        #expect(await transport.sentMessages.count >= 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("serverInfo"))
        }

        await server.stop()
        await transport.disconnect()
    }

    @Test("Initialize hook - rejection")
    func testInitializeHookRejection() async throws {
        let transport = MockTransport()

        let server = Server(name: "TestServer", version: "1.0")

        try await server.start(transport: transport) { clientInfo, _ in
            if clientInfo.name == "BlockedClient" {
                throw MCPError.invalidRequest("Client not allowed")
            }
        }

        // Wait for server to initialize
        try await Task.sleep(for: .milliseconds(10))

        // Queue an initialize request from blocked client
        try await transport.queue(
            request: Initialize.request(
                .init(
                    protocolVersion: Version.latest,
                    capabilities: .init(),
                    clientInfo: .init(name: "BlockedClient", version: "1.0")
                )
            ))

        // Wait for message processing
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sentMessages.count >= 1)

        let messages = await transport.sentMessages
        if let response = messages.first {
            #expect(response.contains("error"))
            #expect(response.contains("Client not allowed"))
        }

        await server.stop()
        await transport.disconnect()
    }

    @Test("JSON-RPC batch processing")
    func testJSONRPCBatchProcessing() async throws {
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let server = Server(name: "TestServer", version: "1.0")

        // Connect transports
        try await clientTransport.connect()
        try await serverTransport.connect()

        // Start receiving messages on client side
        let receiveTask = Task {
            var responses: [String] = []
            for try await data in await clientTransport.receive() {
                if let response = String(data: data, encoding: .utf8) {
                    responses.append(response)
                }
                // Stop after receiving 2 responses (initialize + batch)
                if responses.count == 2 {
                    break
                }
            }
            return responses
        }

        // Start the server
        try await server.start(transport: serverTransport)

        // Initialize the server first
        let initRequest = Initialize.request(
            .init(
                protocolVersion: Version.latest,
                capabilities: .init(),
                clientInfo: .init(name: "TestClient", version: "1.0")
            )
        )
        let initData = try JSONEncoder().encode(AnyRequest(initRequest))
        try await clientTransport.send(initData)

        // Wait for initialization
        try await Task.sleep(for: .milliseconds(100))

        // Create a batch with multiple requests
        let batchJSON = """
            [
                {"jsonrpc":"2.0","id":1,"method":"ping","params":{}},
                {"jsonrpc":"2.0","id":2,"method":"ping","params":{}}
            ]
            """
        let batchData = batchJSON.data(using: .utf8)!
        try await clientTransport.send(batchData)

        // Wait for batch processing
        try await Task.sleep(for: .milliseconds(200))

        // Get responses
        let responses = try await receiveTask.value
        #expect(responses.count == 2)

        // Verify the batch response (second response)
        if responses.count >= 2 {
            let batchResponse = responses[1]

            // Should be an array
            #expect(batchResponse.hasPrefix("["))
            #expect(batchResponse.hasSuffix("]"))

            // Should contain both request IDs
            #expect(batchResponse.contains("\"id\":1"))
            #expect(batchResponse.contains("\"id\":2"))
        }

        await server.stop()
        await clientTransport.disconnect()
        await serverTransport.disconnect()
    }

    @Test(
        "Typed response send failure closes connection and drains all client waiters",
        .timeLimit(.minutes(1))
    )
    func testTypedResponseSendFailureClosesConnectionAndDrainsClientWaiters() async throws {
        let recorder = ServerTestLogRecorder()
        let logger = Logger(label: "mcp.test.response-failure") { _ in
            ServerTestLogHandler(recorder: recorder)
        }
        let (clientTransport, serverTransport) = await MockTransport.createConnectedPair(
            logger: logger
        )
        let server = Server(name: "TestServer", version: "1.0")
        let client = Client(name: "TestClient", version: "1.0")

        try await server.start(transport: serverTransport)
        _ = try await client.connect(transport: clientTransport)

        let gate = ResponseDeliveryRequestGate()
        await server.withMethodHandler(CallTool.self) { _ in
            guard let requestID = Server.currentRequestID else {
                throw MCPError.internalError("Missing request ID")
            }
            try await gate.enter(id: requestID)
            return CallTool.Result()
        }

        let selectedID = ID.number(102)
        await serverTransport.setResponseSendFault(
            .init(
                id: selectedID,
                method: CallTool.name,
                toolName: "response-delivery-test",
                behavior: .throwError(message: "transport-secret-must-not-be-logged")
            )
        )

        let requestIDs = Set((101...105).map(ID.number))
        var contexts: [ID: RequestContext<CallTool.Result>] = [:]
        for requestID in requestIDs {
            contexts[requestID] = try await client.send(
                CallTool.request(
                    id: requestID,
                    .init(
                        name: "response-delivery-test",
                        arguments: ["secret": .string("payload-must-not-be-logged")]
                    )
                )
            )
        }
        let observers = contexts.mapValues { context in
            Task { await observeClientWaiter(context) }
        }

        await gate.waitUntilArrived(requestIDs)
        await gate.release(id: selectedID)
        await serverTransport.waitUntilResponseSendStarted(id: selectedID)

        for requestID in requestIDs {
            #expect(await observers[requestID]?.value == .mcpError(.connectionClosed))
        }
        await server.waitUntilCompleted()

        #expect(await serverTransport.responseSendAttemptCount(for: selectedID) == 1)
        #expect(await serverTransport.disconnectCallCount == 1)
        #expect(await serverTransport.sentMessages.contains(where: { $0.contains("\"id\":102") }) == false)

        let failureEntry = recorder.entries().first(where: {
            $0.metadata["provenance"] == "response_send_failed"
                && $0.metadata["id"] == "102"
        })
        #expect(failureEntry?.metadata["error_code"] == "-32001")
        #expect(failureEntry?.metadata["error_type"]?.contains("MCPError") == true)
        #expect(failureEntry?.metadata.values.contains(where: {
            $0.contains("transport-secret-must-not-be-logged")
                || $0.contains("payload-must-not-be-logged")
        }) == false)

        do {
            _ = try await client.send(
                CallTool.request(
                    id: .number(106),
                    .init(name: "response-delivery-test")
                )
            )
            Issue.record("Expected request 106 to be rejected after terminal response failure")
        } catch let error as MCPError {
            #expect(error == .connectionClosed)
        }
        #expect(await gate.hasArrived(.number(106)) == false)

        await client.disconnect()
        await server.stop()
    }

    @Test(
        "Typed suspended response send expires deadline and drains all client waiters",
        .timeLimit(.minutes(1))
    )
    func testTypedSuspendedResponseSendExpiresDeadlineAndDrainsClientWaiters() async throws {
        let recorder = ServerTestLogRecorder()
        let logger = Logger(label: "mcp.test.response-deadline") { _ in
            ServerTestLogHandler(recorder: recorder)
        }
        let (clientTransport, serverTransport) = await MockTransport.createConnectedPair(
            logger: logger
        )
        let configuration = Server.Configuration(responseSendTimeout: .seconds(30))
        let legacyConfiguration = try JSONDecoder().decode(
            Server.Configuration.self,
            from: Data(#"{"strict":true}"#.utf8)
        )
        #expect(Server.Configuration.default.responseSendTimeout == .seconds(30))
        #expect(configuration.responseSendTimeout == .seconds(30))
        #expect(legacyConfiguration.strict == true)
        #expect(legacyConfiguration.responseSendTimeout == .seconds(30))

        let server = Server(
            name: "TestServer",
            version: "1.0",
            configuration: configuration
        )
        let deadline = ManualResponseSendDeadline()
        let client = Client(name: "TestClient", version: "1.0")

        try await server.start(transport: serverTransport)
        _ = try await client.connect(transport: clientTransport)
        await server.setResponseSendDeadlineSleepForTesting { duration in
            try await deadline.sleep(for: duration)
        }

        let gate = ResponseDeliveryRequestGate()
        await server.withMethodHandler(CallTool.self) { _ in
            guard let requestID = Server.currentRequestID else {
                throw MCPError.internalError("Missing request ID")
            }
            try await gate.enter(id: requestID)
            return CallTool.Result()
        }

        let selectedID = ID.number(102)
        await serverTransport.setResponseSendFault(
            .init(
                id: selectedID,
                method: CallTool.name,
                toolName: "response-delivery-test",
                behavior: .suspend
            )
        )

        let requestIDs = Set((101...105).map(ID.number))
        var contexts: [ID: RequestContext<CallTool.Result>] = [:]
        for requestID in requestIDs {
            contexts[requestID] = try await client.send(
                CallTool.request(
                    id: requestID,
                    .init(
                        name: "response-delivery-test",
                        arguments: ["secret": .string("payload-must-not-be-logged")]
                    )
                )
            )
        }
        let observers = contexts.mapValues { context in
            Task { await observeClientWaiter(context) }
        }

        await gate.waitUntilArrived(requestIDs)
        await gate.release(id: selectedID)
        await serverTransport.waitUntilResponseSendStarted(id: selectedID)
        await deadline.waitUntilArmed()
        await deadline.expire()

        for requestID in requestIDs {
            #expect(await observers[requestID]?.value == .mcpError(.connectionClosed))
        }
        await server.waitUntilCompleted()

        #expect(await serverTransport.responseSendAttemptCount(for: selectedID) == 1)
        #expect(await serverTransport.disconnectCallCount == 1)

        let deadlineEntry = recorder.entries().first(where: {
            $0.metadata["provenance"] == "response_send_deadline_exceeded"
        })
        #expect(deadlineEntry?.message == "Failed to send JSON-RPC response")
        #expect(deadlineEntry?.metadata["id"] == "102")
        #expect(deadlineEntry?.metadata["method"] == CallTool.name)
        #expect(deadlineEntry?.metadata["tool"] == "response-delivery-test")
        #expect(deadlineEntry?.metadata["phase"] == "single_response")
        #expect(deadlineEntry?.metadata["response_count"] == "1")
        #expect(deadlineEntry?.metadata["timeout_seconds"] == "30.0")
        #expect(Int(deadlineEntry?.metadata["bytes"] ?? "") != nil)
        #expect(Double(deadlineEntry?.metadata["elapsed_seconds"] ?? "") != nil)
        #expect(deadlineEntry?.metadata.values.contains(where: {
            $0.contains("payload-must-not-be-logged")
        }) == false)

        do {
            _ = try await client.send(
                CallTool.request(
                    id: .number(106),
                    .init(name: "response-delivery-test")
                )
            )
            Issue.record("Expected request 106 to be rejected after response deadline expiry")
        } catch let error as MCPError {
            #expect(error == .connectionClosed)
        }
        #expect(await gate.hasArrived(.number(106)) == false)

        await client.disconnect()
        await server.stop()
    }

    @Test("Response send failure after handler success is single-attempt and terminal")
    func testResponseSendFailureAfterHandlerSuccessDoesNotSendSecondError() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        try await transport.queue(request: Ping.request(id: .number(1)))
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Batch response send failure is single-attempt and terminal")
    func testBatchResponseSendFailureIsSingleAttemptAndTerminal() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        let batchJSON = """
            [
                {"jsonrpc":"2.0","id":1,"method":"ping","params":{}},
                {"jsonrpc":"2.0","id":2,"method":"ping","params":{}}
            ]
            """
        await transport.queue(data: batchJSON.data(using: .utf8)!)
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Parse error response send failure closes transport")
    func testParseErrorResponseSendFailureClosesTransport() async throws {
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        await transport.queue(data: Data("not-json".utf8))
        try await Task.sleep(for: .milliseconds(200))

        #expect(await transport.sendAttempts == 1)
        #expect(await transport.sentMessages.isEmpty)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Response send failure prevents later request handling")
    func testResponseSendFailurePreventsLaterRequestHandling() async throws {
        actor Counter {
            private(set) var count = 0
            func increment() { count += 1 }
        }

        let counter = Counter()
        let transport = MockTransport()
        await transport.setFailOnSendAttempt(1)

        let server = Server(name: "TestServer", version: "1.0")
        await server.withMethodHandler(ListTools.self) { _ in
            await counter.increment()
            return ListTools.Result(tools: [])
        }
        try await transport.queue(request: ListTools.request(id: .number(1), .init()))
        try await transport.queue(request: ListTools.request(id: .number(2), .init()))

        try await server.start(transport: transport)
        try await Task.sleep(for: .milliseconds(300))

        #expect(await counter.count == 1)
        #expect(await transport.sendAttempts == 1)
        #expect(await transport.isConnected == false)

        await server.stop()
    }

    @Test("Long-running request does not block later request")
    func testLongRunningRequestDoesNotBlockLaterRequest() async throws {
        actor Counter {
            private(set) var count = 0
            func increment() { count += 1 }
        }

        let counter = Counter()
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")

        await server.withMethodHandler(SlowServerTestMethod.self) { _ in
            try await Task.sleep(for: .milliseconds(300))
            return Empty()
        }
        await server.withMethodHandler(ListTools.self) { _ in
            await counter.increment()
            return ListTools.Result(tools: [])
        }

        try await transport.queue(request: SlowServerTestMethod.request(id: .number(1)))
        try await transport.queue(request: ListTools.request(id: .number(2), .init()))

        try await server.start(transport: transport)
        try await Task.sleep(for: .milliseconds(100))

        #expect(await counter.count == 1)

        await server.stop()
    }

    @Test("Clean EOF drains server-initiated pending responses")
    func testCleanEOFDrainsServerInitiatedPendingResponses() async throws {
        let transport = MockTransport()
        let server = Server(name: "TestServer", version: "1.0")
        try await server.start(transport: transport)

        let rootsTask = Task {
            try await server.listRoots()
        }

        #expect(await waitForSentMessage(containing: ListRoots.name, transport: transport))
        await transport.finishReceiving()

        let result = await awaitResult {
            try await rootsTask.value
        }

        switch result {
        case .success:
            #expect(Bool(false), "Expected pending server request to fail after clean EOF")
        case .failure(let error as MCPError):
            #expect(error == .connectionClosed)
        case .failure(let error):
            #expect(Bool(false), "Expected MCPError.connectionClosed, got \(error)")
        }

        #expect(await transport.isConnected == false)
        await server.stop()
    }

    private func waitForSentMessage(
        containing needle: String,
        transport: MockTransport,
        attempts: Int = 100
    ) async -> Bool {
        for _ in 0..<attempts {
            if await transport.sentMessages.contains(where: { $0.contains(needle) }) {
                return true
            }
            try? await Task.sleep(for: .milliseconds(5))
        }
        return await transport.sentMessages.contains(where: { $0.contains(needle) })
    }

    private func awaitResult<T: Sendable>(
        timeout: Duration = .milliseconds(500),
        _ operation: @escaping @Sendable () async throws -> T
    ) async -> Result<T, Swift.Error> {
        await withTaskGroup(of: Result<T, Swift.Error>.self) { group in
            group.addTask {
                do {
                    return .success(try await operation())
                } catch {
                    return .failure(error)
                }
            }
            group.addTask {
                try? await Task.sleep(for: timeout)
                return .failure(MCPError.internalError("Timed out waiting for result"))
            }

            let result = await group.next()!
            group.cancelAll()
            return result
        }
    }
}
