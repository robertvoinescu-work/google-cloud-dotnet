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

using Google.Cloud.Spanner.Data.CommonTesting;
using System;
using Xunit;

namespace Google.Cloud.Spanner.Data.IntegrationTests
{
    [CollectionDefinition(nameof(BatchMutationTableFixture))]
    public class BatchMutationTableFixture : SpannerTableFixture, ICollectionFixture<BatchMutationTableFixture>
    {
        public BatchMutationTableFixture() : base("BatchMutationTest")
        {
        }

        protected override void CreateTable()
        {
            using var connection = GetConnection();
            connection.Open();
            var command = connection.CreateDdlCommand(
                $"CREATE TABLE {TableName} (Key STRING(36) NOT NULL, Value STRING(MAX)) PRIMARY KEY (Key)");
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Creates a random key and inserts a row with that key.
        /// </summary>
        /// <returns>The key for the new row.</returns>
        public string CreateTestRows()
        {
            var key = Guid.NewGuid().ToString();
            using var connection = GetConnection();
            connection.Open();
            using var cmd = connection.CreateInsertCommand(TableName, new SpannerParameterCollection
            {
                { "Key", SpannerDbType.String, key },
                { "Value", SpannerDbType.String, "initial" }
            });
            cmd.ExecuteNonQuery();
            return key;
        }
    }
}
