// Copyright 2024 Google LLC
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
using Google.Cloud.Spanner.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Google.Cloud.Spanner.Data
{
    /// <summary>
    /// Represents a batch of mutation groups to execute against a Spanner database.
    /// All mutations in a group are committed atomically. However, the individual groups
    /// are committed non-atomically in an unspecified order.
    /// </summary>
    public sealed class SpannerBatchMutationCommand
    {
        private readonly SpannerConnection _connection;
        private readonly List<IEnumerable<Mutation>> _mutationGroups = new List<IEnumerable<Mutation>>();

        internal SpannerBatchMutationCommand(SpannerConnection connection) =>
            _connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));

        /// <summary>
        /// The transaction to use when executing this command.
        /// This must be null, as BatchWrite operations do not participate in transactions.
        /// </summary>
        public SpannerTransaction Transaction { get; set; }

        /// <summary>
        /// Adds a mutation group to the batch. Each group is a collection of mutations
        /// that will be committed atomically.
        /// </summary>
        /// <param name="mutations">
        /// The mutations to add as an atomic group. Must not be null or empty.
        /// </param>
        public void Add(IEnumerable<Mutation> mutations)
        {
            GaxPreconditions.CheckNotNull(mutations, nameof(mutations));
            _mutationGroups.Add(mutations.ToList());
        }

        /// <summary>
        /// Adds a mutation group to the batch. Each group is a collection of mutations
        /// that will be committed atomically.
        /// </summary>
        /// <param name="mutations">
        /// The mutations to add as an atomic group. Must not be null or empty.
        /// </param>
        public void Add(params Mutation[] mutations) => Add((IEnumerable<Mutation>)mutations);

        /// <summary>
        /// Executes the batch of mutation groups.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token for the operation.</param>
        /// <returns>
        /// An asynchronous enumerable of <see cref="BatchWriteResponse"/> messages,
        /// each corresponding to a successfully committed mutation group.
        /// </returns>
        public IAsyncEnumerable<BatchWriteResponse> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction is not null)
            {
                throw new InvalidOperationException($"A transaction may not be used with {nameof(SpannerBatchMutationCommand)}.");
            }
            if (_mutationGroups.Count == 0)
            {
                return EmptyStream();
            }

            return ExecuteStreamAsync(cancellationToken);

            static async IAsyncEnumerable<BatchWriteResponse> EmptyStream([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                await Task.Yield();
                yield break;
            }
        }

        private async IAsyncEnumerable<BatchWriteResponse> ExecuteStreamAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var request = new BatchWriteRequest();
            foreach (var group in _mutationGroups)
            {
                request.MutationGroups.Add(new BatchWriteRequest.Types.MutationGroup { Mutations = { group } });
            }

            var session = await _connection.AcquireSessionAsync(null, cancellationToken, out _).ConfigureAwait(false);
            var stream = session.BatchWrite(request, null);
            var asyncEnumerator = stream.GrpcCall.ResponseStream.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
            try
            {
                while (await asyncEnumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    yield return asyncEnumerator.Current;
                }
            }
            finally
            {
                await asyncEnumerator.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
