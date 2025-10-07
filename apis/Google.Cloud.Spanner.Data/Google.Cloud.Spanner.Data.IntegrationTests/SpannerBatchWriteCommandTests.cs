// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.Data.IntegrationTests;

[Collection(nameof(BatchWriteTableFixture))]
public class SpannerBatchWriteCommandTests
{
    private readonly BatchWriteTableFixture _fixture;

    public SpannerBatchWriteCommandTests(BatchWriteTableFixture fixture) =>
        _fixture = fixture;

    [Fact]
    public async Task ExecuteAsync_Success()
    {
        using var connection = _fixture.GetConnection();
        var command = connection.CreateBatchWriteCommand();
        var key1 = Guid.NewGuid().ToString();
        var key2 = Guid.NewGuid().ToString();

        var insertCommand1 = connection.CreateInsertCommand(_fixture.TableName, new SpannerParameterCollection { { _fixture.KeyName, SpannerDbType.String, key1 } });
        var insertCommand2 = connection.CreateInsertCommand(_fixture.TableName, new SpannerParameterCollection { { _fixture.KeyName, SpannerDbType.String, key2 } });

        command.Add(new[] { insertCommand1, insertCommand2 });

        var responses = await command.ExecuteAsync().ToListAsync();

        Assert.Single(responses);
        var response = responses[0];
        Assert.Equal(0, response.Status.Code); // OK
        Assert.Single(response.Indexes);
        Assert.Equal(0, response.Indexes[0]);

        using var snapshot = await connection.BeginTransactionAsync();
        var readCommand = connection.CreateReadCommand(_fixture.TableName, ReadOptions.FromColumns(_fixture.KeyName), KeySet.All);
        readCommand.Transaction = snapshot;
        using var reader = await readCommand.ExecuteReaderAsync();
        var keys = new HashSet<string>();
        while(await reader.ReadAsync())
        {
            keys.Add(reader.GetString(0));
        }
        Assert.Contains(key1, keys);
        Assert.Contains(key2, keys);
    }

    [Fact]
    public async Task ExecuteAsync_PartialFailure()
    {
        using var connection = _fixture.GetConnection();
        var command = connection.CreateBatchWriteCommand();
        var conflictKey = Guid.NewGuid().ToString();
        var successKey = Guid.NewGuid().ToString();

        var insertConflict1 = connection.CreateInsertCommand(_fixture.TableName, new SpannerParameterCollection { { _fixture.KeyName, SpannerDbType.String, conflictKey } });
        var insertConflict2 = connection.CreateInsertCommand(_fixture.TableName, new SpannerParameterCollection { { _fixture.KeyName, SpannerDbType.String, conflictKey } });
        var insertSuccess = connection.CreateInsertCommand(_fixture.TableName, new SpannerParameterCollection { { _fixture.KeyName, SpannerDbType.String, successKey } });

        // This group will fail due to a primary key conflict.
        command.Add(new[] { insertConflict1, insertConflict2 });
        // This group will succeed.
        command.Add(insertSuccess);

        var responses = await command.ExecuteAsync().ToListAsync();

        Assert.Equal(2, responses.Count);
        var failedResponse = responses.Single(r => r.Status.Code != 0);
        var successResponse = responses.Single(r => r.Status.Code == 0);

        Assert.Single(failedResponse.Indexes);
        Assert.Equal(0, failedResponse.Indexes[0]);
        Assert.Single(successResponse.Indexes);
        Assert.Equal(1, successResponse.Indexes[0]);

        using var snapshot = await connection.BeginTransactionAsync();
        var readCommand = connection.CreateReadCommand(_fixture.TableName, ReadOptions.FromColumns(_fixture.KeyName), KeySet.All);
        readCommand.Transaction = snapshot;
        using var reader = await readCommand.ExecuteReaderAsync();
        var keys = new HashSet<string>();
        while (await reader.ReadAsync())
        {
            keys.Add(reader.GetString(0));
        }
        Assert.Contains(successKey, keys);
        Assert.DoesNotContain(conflictKey, keys);
    }

    [Fact]
    public async Task ExecuteAsync_EmptyCommand()
    {
        using var connection = _fixture.GetConnection();
        var command = connection.CreateBatchWriteCommand();

        var responses = await command.ExecuteAsync().ToListAsync();

        Assert.Empty(responses);
    }

    [Fact]
    public void Add_InvalidCommandType_Throws()
    {
        using var connection = _fixture.GetConnection();
        var command = connection.CreateBatchWriteCommand();
        var selectCommand = connection.CreateSelectCommand($"SELECT * FROM {_fixture.TableName}");

        Assert.Throws<ArgumentOutOfRangeException>(() => command.Add(selectCommand));
    }
}
