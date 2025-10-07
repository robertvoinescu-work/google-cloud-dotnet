// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Spanner.V1;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using static Google.Cloud.Spanner.V1.BatchWriteRequest.Types;

namespace Google.Cloud.Spanner.Data;

/// <summary>
/// Represents a batch of mutation groups to be sent to Spanner via the BatchWrite RPC.
/// Each group of mutations is committed atomically, but independent of other groups.
/// This command is non-transactional and cannot be used with an explicit transaction.
/// </summary>
public sealed class SpannerBatchWriteCommand
{
    // Visible for testing
    internal List<MutationGroup> MutationGroups { get; } = [];

    /// <summary>
    /// The connection to the data source. This is never null.
    /// </summary>
    public SpannerConnection Connection { get; }

    /// <summary>
    /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
    /// Defaults to the timeout from the connection string.
    /// </summary>
    public int CommandTimeout { get; set; }

    /// <summary>
    /// The statement tag to send to Cloud Spanner for this command.
    /// </summary>
    public string Tag { get; set; }

    /// <summary>
    /// The RPC priority to use for this command. The default priority is Unspecified.
    /// </summary>
    public Priority Priority { get; set; }

    /// <summary>
    /// If set to true then any change streams monitoring columns modified
    /// by transactions will capture the updates made within that transaction.
    /// </summary>
    public bool ExcludeTxnFromChangeStream {get; set;}

    internal SpannerBatchWriteCommand(SpannerConnection connection)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
        CommandTimeout = connection.Builder.Timeout;
    }

    /// <summary>
    /// Adds a command or multiple commands as a new mutation group to be resolved atomically.
    /// </summary>
    /// <param name="commands"> The command or commands to add as a single mutation group </param>
    public void Add(params SpannerCommand[] commands)=> Add(commands.AsEnumerable());

    /// <summary>
    /// Adds a collection of commands as new mutation group to be resolved atomically to be resolved atomically.
    /// </summary>
    /// <param name="commands"> The commands to add as a single mutation group.</param>
    public void Add(IEnumerable<SpannerCommand> commands)
    {
        var groupAsList = GaxPreconditions.CheckNotNull(commands, nameof(commands)).ToList();
        GaxPreconditions.CheckArgument(groupAsList.Any(), nameof(commands), "Command group cannot be empty.");

        var mutations = new List<Mutation>();
        foreach (SpannerCommand cmd in groupAsList)
        {
            mutations.Add(cmd.AsMutation());
        }

        // These mutations will be resolved as a single mutation group.
        MutationGroups.Add(new MutationGroup
        {
            Mutations = { mutations }
        });
    }

    /// <summary>
    /// Executes the batch of mutation groups using the BatchWrite RPC, streaming the results.
    /// </summary>
    public async IAsyncEnumerable<BatchWriteResponse> ExecuteAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (MutationGroups.Count == 0)
        {
            yield break;
        }

        var session = await Connection.AcquireSessionAsync(null, cancellationToken, out _).ConfigureAwait(false);
        try
        {
            BatchWriteRequest request = CreateBatchWriteRequest();
            CallSettings callSettings = Connection.CreateCallSettings(null, CommandTimeout, cancellationToken);
            IAsyncEnumerable<BatchWriteResponse> responseStream = session.BatchWriteAsync(request, callSettings);

            await foreach (BatchWriteResponse response in responseStream.ConfigureAwait(false))
            {
                yield return response;
            }
        }
        finally
        {
            session.ReleaseToPool(forceDelete: false);
        }
    }

    private BatchWriteRequest CreateBatchWriteRequest()
    {
        var request = new BatchWriteRequest()
        {
            ExcludeTxnFromChangeStreams = ExcludeTxnFromChangeStream
        };

        if (Tag != null)
        {
            request.RequestOptions = new RequestOptions { RequestTag = Tag };
        }

        if (Priority != Priority.Unspecified)
        {
            request.RequestOptions ??= new RequestOptions();
            request.RequestOptions.Priority = PriorityConverter.ToProto(Priority);
        }

        request.MutationGroups.AddRange(MutationGroups);
        return request;
    }
}
