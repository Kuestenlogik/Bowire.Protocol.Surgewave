// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Auth;
using Kuestenlogik.Bowire.Protocol.Surgewave;
using Kuestenlogik.Surgewave.Client;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

/// <summary>
/// Unit tests for the marker → builder bridge in
/// <see cref="SurgewaveSecurityConfig"/>. Verifies that both magic
/// metadata keys (<c>__bowireMtls__</c> + <c>__bowireKafkaSasl__</c>)
/// get stripped from the dict on the way through, so the Kafka
/// produce/consume path can't accidentally leak them as wire headers.
/// </summary>
public class SurgewaveSecurityConfigTests
{
    [Fact]
    public void Apply_NoMarkers_ReturnsBuilderUnchangedAndNullMetadata()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var (resultBuilder, sanitised) = SurgewaveSecurityConfig.Apply(builder, metadata: null);
        Assert.Same(builder, resultBuilder);
        Assert.Null(sanitised);
    }

    [Fact]
    public void Apply_StripsMtlsMarker_FromSanitisedDict()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var meta = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MtlsConfig.MtlsMarkerKey] = """{"certificate":"CERT","privateKey":"KEY"}""",
            ["key"] = "user-42",
        };

        var (_, sanitised) = SurgewaveSecurityConfig.Apply(builder, meta);

        Assert.NotNull(sanitised);
        Assert.False(sanitised!.ContainsKey(MtlsConfig.MtlsMarkerKey));
        Assert.Equal("user-42", sanitised["key"]);
    }

    [Fact]
    public void Apply_StripsSaslMarker_FromSanitisedDict()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var meta = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveSecurityConfig.SaslMarkerKey] = """{"mechanism":"PLAIN","username":"alice","password":"s3cret"}""",
            ["partition"] = "0",
        };

        var (_, sanitised) = SurgewaveSecurityConfig.Apply(builder, meta);

        Assert.NotNull(sanitised);
        Assert.False(sanitised!.ContainsKey(SurgewaveSecurityConfig.SaslMarkerKey));
        Assert.Equal("0", sanitised["partition"]);
    }

    [Fact]
    public void Apply_BothMarkers_StripsBoth()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var meta = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MtlsConfig.MtlsMarkerKey] = """{"certificate":"CERT","privateKey":"KEY"}""",
            [SurgewaveSecurityConfig.SaslMarkerKey] = """{"mechanism":"PLAIN","username":"alice","password":"s"}""",
            ["other"] = "value",
        };

        var (_, sanitised) = SurgewaveSecurityConfig.Apply(builder, meta);

        Assert.NotNull(sanitised);
        Assert.Single(sanitised!);
        Assert.Equal("value", sanitised["other"]);
    }

    [Fact]
    public void SaslConfig_TryParse_RejectsMissingFields()
    {
        Assert.Null(SurgewaveSecurityConfig.SaslConfig.TryParse("""{"mechanism":"PLAIN"}"""));
        Assert.Null(SurgewaveSecurityConfig.SaslConfig.TryParse("""{"username":"alice"}"""));
        Assert.Null(SurgewaveSecurityConfig.SaslConfig.TryParse("not json"));
    }

    [Fact]
    public void SaslConfig_TryParse_DefaultsMechanismToPlain()
    {
        var cfg = SurgewaveSecurityConfig.SaslConfig.TryParse("""{"username":"a","password":"b"}""");
        Assert.NotNull(cfg);
        Assert.Equal("PLAIN", cfg!.Mechanism);
    }
}
