// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Google.Cloud.Spanner.V1.Tests;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace Google.Cloud.Spanner.Data.Tests;

public class SpannerBatchWriteCommandTests
{
    [Fact]
    public void ConnectionConstructor()
    {
        var connection = new SpannerConnection();
        var command = new SpannerBatchWriteCommand(connection);

        Assert.Empty(command.MutationGroups);
        Assert.Same(connection, command.Connection);
    }

    [Fact]
    public void CommandPriorityDefaultsToUnspecified()
    {
        SpannerClient spannerClientMock = SpannerClientHelpers.CreateMockClient(Logger.DefaultLogger);
        spannerClientMock
            .SetupBatchCreateSessionsAsync();
        SpannerConnection connection = SpannerCommandTests.BuildSpannerConnection(spannerClientMock);

        var command = connection.CreateBatchWriteCommand();
        Assert.Equal(Priority.Unspecified, command.Priority);
    }

    [Fact]
    public async Task CommandIncludesPriority()
    {
        var priority = Priority.High;
        SpannerClient spannerClientMock = SpannerClientHelpers.CreateMockClient(Logger.DefaultLogger);
        spannerClientMock
            .SetupBatchCreateSessionsAsync();
        SpannerConnection connection = SpannerCommandTests.BuildSpannerConnection(spannerClientMock);
        var command = connection.CreateBatchWriteCommand();

        command.Priority = priority;
        await foreach (var result in command.ExecuteAsync())
        {
            // do nothing just let it process
        }

        spannerClientMock.Received(1).BatchWrite(
            Arg.Is<BatchWriteRequest>(request => request.RequestOptions.Priority == PriorityConverter.ToProto(priority)),
            Arg.Any<CallSettings>());
    }

    [Fact]
    public async Task CommandIncludesRequestTag()
    {
        var requestTag = "request-tag-1";
        SpannerClient spannerClientMock = SpannerClientHelpers.CreateMockClient(Logger.DefaultLogger);
        spannerClientMock
            .SetupBatchCreateSessionsAsync()
            .SetupExecuteBatchDmlAsync()
            .SetupCommitAsync();
        SpannerConnection connection = SpannerCommandTests.BuildSpannerConnection(spannerClientMock);
        SpannerBatchWriteCommand command = connection.CreateBatchWriteCommand();

        command.Tag = requestTag;
        await foreach (var result in command.ExecuteAsync())
        {
            // do nothing just let it process
        }

        spannerClientMock.Received(1).BatchWrite(
            Arg.Is<BatchWriteRequest>(request => request.RequestOptions.RequestTag == requestTag),
            Arg.Any<CallSettings>());
    }

    [Fact]
    public async Task CommandIncludesExcludeTxnFromChangeStream()
    {
        SpannerClient spannerClientMock = SpannerClientHelpers.CreateMockClient(Logger.DefaultLogger);
        spannerClientMock
            .SetupBatchCreateSessionsAsync()
            .SetupExecuteBatchDmlAsync()
            .SetupCommitAsync();
        SpannerConnection connection = SpannerCommandTests.BuildSpannerConnection(spannerClientMock);
        SpannerBatchWriteCommand command = connection.CreateBatchWriteCommand();

        command.ExcludeTxnFromChangeStream = true;
        await foreach (var result in command.ExecuteAsync())
        {
            // do nothing just let it process
        }

        spannerClientMock.Received(1).BatchWrite(
            Arg.Is<BatchWriteRequest>(request => request.ExcludeTxnFromChangeStreams),
            Arg.Any<CallSettings>());
    }
}
