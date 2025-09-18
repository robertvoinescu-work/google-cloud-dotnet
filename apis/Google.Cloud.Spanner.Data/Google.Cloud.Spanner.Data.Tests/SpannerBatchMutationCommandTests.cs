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

using Google.Cloud.Spanner.V1;
using NSubstitute;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.Data.Tests
{
    public class SpannerBatchMutationCommandTests
    {
        [Fact]
        public void Add_Validations()
        {
            var connection = Substitute.For<SpannerConnection>();
            var command = new SpannerBatchMutationCommand(connection);

            Assert.Throws<ArgumentNullException>(() => command.Add(null));
        }

        [Fact]
        public async Task ExecuteAsync_ThrowsWithTransaction()
        {
            var connection = Substitute.For<SpannerConnection>();
            var command = new SpannerBatchMutationCommand(connection)
            {
                Transaction = Substitute.For<SpannerTransaction>()
            };
            command.Add(new[] { new Mutation() });

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => command.ExecuteAsync().ToListAsync().AsTask());
            Assert.Contains(nameof(SpannerBatchMutationCommand), exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_NoMutations_ReturnsEmptyStream()
        {
            var connection = Substitute.For<SpannerConnection>();
            var command = new SpannerBatchMutationCommand(connection);

            var responses = await command.ExecuteAsync().ToListAsync();
            Assert.Empty(responses);
        }
    }
}
