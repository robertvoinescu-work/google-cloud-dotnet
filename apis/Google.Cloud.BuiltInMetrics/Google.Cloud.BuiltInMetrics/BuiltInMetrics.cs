// Copyright 2026 Google LLC
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
using Google.Apis.Auth.OAuth2;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.BuiltInMetrics;

/// <summary>
/// Entry point for enabling and exporting Google Cloud client built-in metrics telemetry.
/// </summary>
public static class BuiltInMetrics
{
    private static readonly Uri s_metricsEndpoint = new("https://telemetry.googleapis.com/v1/metrics");
    // The minimal privelaged auth scope required to emit telmemetry
    private const string MonitoringAuthScope = "https://www.googleapis.com/auth/monitoring.write";

    /// <summary>
    /// Enables Google Cloud client built-in metrics collection and starts exporting telemetry to Google Cloud Monitoring.
    /// </summary>
    /// <param name="credential">Optional Google credential. If null, Application Default Credentials (ADC) will be used.</param>
    /// <returns>An <see cref="IDisposable"/> session. Disposing it flushes pending telemetry and stops collection.</returns>
    public static IDisposable Enable(GoogleCredential credential = null)
    {
        var meterProvider = Sdk.CreateMeterProviderBuilder()
            .AddGoogleCloudBuiltInMetrics(credential)
            .Build();
        return new BuiltInMetricsSession(meterProvider);
    }

    /// <summary>
    /// Asynchronously enables Google Cloud client built-in metrics collection and starts exporting telemetry to Google Cloud Monitoring.
    /// </summary>
    /// <param name="credential">Optional Google credential. If null, Application Default Credentials (ADC) will be used.</param>
    /// <param name="cancellationToken">Cancellation token for credential resolution.</param>
    /// <returns>A task returning an <see cref="IDisposable"/> session. Disposing it flushes pending telemetry and stops collection.</returns>
    public static async Task<IDisposable> EnableAsync(GoogleCredential credential = null, CancellationToken cancellationToken = default)
    {
        var builder = Sdk.CreateMeterProviderBuilder();
        await builder.AddGoogleCloudBuiltInMetricsAsync(credential, cancellationToken).ConfigureAwait(false);
        return new BuiltInMetricsSession(builder.Build());
    }

    /// <summary>
    /// Helper method to configure an existing <see cref="MeterProviderBuilder"/> with the Google Cloud built-in metrics OTLP exporter.
    /// </summary>
    internal static MeterProviderBuilder AddGoogleCloudBuiltInMetrics(
        this MeterProviderBuilder builder,
        GoogleCredential credential = null)
    {
        GaxPreconditions.CheckNotNull(builder, nameof(builder));
        return ConfigureOtlpExporter(builder, GetDefaultCredential(credential));
    }

    /// <summary>
    /// Helper method to configure an existing <see cref="MeterProviderBuilder"/> with the Google Cloud built-in metrics OTLP exporter.
    /// </summary>
    internal static async Task<MeterProviderBuilder> AddGoogleCloudBuiltInMetricsAsync(
        this MeterProviderBuilder builder,
        GoogleCredential credential = null,
        CancellationToken cancellationToken = default)
    {
        GaxPreconditions.CheckNotNull(builder, nameof(builder));
        var resolvedCredential = await GetDefaultCredentialAsync(credential, cancellationToken).ConfigureAwait(false);
        return ConfigureOtlpExporter(builder, resolvedCredential);
    }

    private static MeterProviderBuilder ConfigureOtlpExporter(MeterProviderBuilder builder, GoogleCredential credential)
    {
        SpannerMetricsConfiguration.Instance.Configure(builder);
        return builder.AddOtlpExporter(exporterOptions =>
        {
            exporterOptions.Endpoint = s_metricsEndpoint;
            exporterOptions.Protocol = OtlpExportProtocol.HttpProtobuf;
            exporterOptions.HttpClientFactory = () => new HttpClient(new AuthHeaderHandler(credential));
        });
    }

    private static GoogleCredential GetDefaultCredential(GoogleCredential credential)
    {
        credential ??= GoogleCredential.GetApplicationDefault();
        if (credential.IsCreateScopedRequired)
        {
            // We attach the required monitoring scope if the provided credential has no scope such as in the case of ADC.
            credential = credential.CreateScoped(MonitoringAuthScope);
        }
        return credential;
    }

    private static async Task<GoogleCredential> GetDefaultCredentialAsync(GoogleCredential credential, CancellationToken cancellationToken)
    {
        credential ??= await GoogleCredential.GetApplicationDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (credential.IsCreateScopedRequired)
        {
            credential = credential.CreateScoped(MonitoringAuthScope);
        }
        return credential;
    }

    /// <summary>
    /// Attaches a Bearer token to outbound requests. This is required because the OTLP
    /// metrics endpoint requires Bearer token authorization and tokens must be dynamically refreshed as they expire.
    /// </summary>
    private sealed class AuthHeaderHandler : DelegatingHandler
    {
        private readonly GoogleCredential _credential;

        public AuthHeaderHandler(GoogleCredential credential)
        {
            _credential = GaxPreconditions.CheckNotNull(credential, nameof(credential));
            InnerHandler = new HttpClientHandler();
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            string accessToken = await ((ITokenAccess)_credential).GetAccessTokenForRequestAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Restricts the underlying <see cref="MeterProvider"/> to only expose <see cref="IDisposable"/>.
    /// This is required to prevent callers from directly manipulating or misconfiguring the provider while still allowing them to flush and stop collection.
    /// </summary>
    internal sealed class BuiltInMetricsSession : IDisposable
    {
        internal MeterProvider MeterProvider { get; }

        public BuiltInMetricsSession(MeterProvider meterProvider) =>
            MeterProvider = GaxPreconditions.CheckNotNull(meterProvider, nameof(meterProvider));

        public void Dispose() => MeterProvider.Dispose();
    }
}
