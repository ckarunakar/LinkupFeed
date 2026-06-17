using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace LinkupFeed
{
    // Ashby exposes a public JSON posting API per company:
    //   GET https://api.ashbyhq.com/posting-api/job-board/{company}?includeCompensation=true
        // The payload normally includes: id, title, department, team, location,
        // secondaryLocations, employmentType, workplaceType, descriptionPlain,
        // descriptionHtml, jobUrl, applyUrl, and publishedDate.
    internal class AshbyScraper
    {
        private const int SOURCE_ID = 58;

        // Curated Ashby job-board slugs. Dead or moved slugs fail fast and are skipped.
        private static readonly (string Slug, string Company)[] Companies =
        {
            ("ashby", "Ashby"),
            ("openai", "OpenAI"),
            ("ramp", "Ramp"),
            ("notion", "Notion"),
            ("perplexity", "Perplexity"),
            ("cursor", "Cursor"),
            ("decagon", "Decagon"),
            ("harvey", "Harvey"),
            ("sierra", "Sierra"),
            ("cognition", "Cognition"),
            ("linear", "Linear"),
            ("claylabs", "Clay"),
            ("mercury", "Mercury")
        };

        private static readonly HttpClient _http = new HttpClient(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip |
                                     DecompressionMethods.Deflate |
                                     DecompressionMethods.Brotli
        })
        {
            Timeout = TimeSpan.FromSeconds(60),
            DefaultRequestHeaders =
            {
                { "Accept", "application/json" },
                { "User-Agent", "ITJobCafe-Scraper/1.0" }
            }
        };

        private static readonly Regex HtmlStripPattern =
            new Regex(@"<[^>]+>", RegexOptions.Compiled);
        private static readonly Regex MultiSpacePattern =
            new Regex(@"\s{2,}", RegexOptions.Compiled);

        public async Task<List<ScrapedJob>> FetchJobsAsync(string onlySlug = null)
        {
            var results = new List<ScrapedJob>();
            var companies = Companies.AsEnumerable();

            if (!string.IsNullOrWhiteSpace(onlySlug))
            {
                companies = companies.Where(c =>
                    string.Equals(c.Slug, onlySlug, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(c.Company, onlySlug, StringComparison.OrdinalIgnoreCase));
            }

            foreach (var (slug, companyName) in companies)
            {
                try
                {
                    var url = $"https://api.ashbyhq.com/posting-api/job-board/{slug}";
                    Console.WriteLine($"[Ashby] {slug} -> starting");

                    var json = await FetchJsonAsync(url, slug);
                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;

                    if (!root.TryGetProperty("jobs", out var jobsEl) ||
                        jobsEl.ValueKind != JsonValueKind.Array)
                    {
                        Console.WriteLine($"[Ashby] {slug} -> unexpected payload shape");
                        continue;
                    }

                    int added = 0;
                    foreach (var j in jobsEl.EnumerateArray())
                    {
                        if (j.TryGetProperty("isListed", out var isListed) &&
                            isListed.ValueKind == JsonValueKind.False)
                            continue;

                        var title = GetText(j, "title");
                        var location = GetLocation(j);
                        var workplace = GetText(j, "workplaceType") ?? "";
                        bool isRemote =
                            GetBool(j, "isRemote") == true ||
                            workplace.IndexOf("remote", StringComparison.OrdinalIgnoreCase) >= 0 ||
                            location.IndexOf("remote", StringComparison.OrdinalIgnoreCase) >= 0;

                        // Keep the same behavior as Lever: remote jobs are eligible even
                        // when the location text is broad, then Program.cs applies IT filtering.
                        if (!UsLocationFilter.IsUs(location) && !isRemote) continue;

                        var description = GetText(j, "descriptionPlain")
                                       ?? StripHtml(GetText(j, "descriptionHtml"))
                                       ?? StripHtml(GetText(j, "description"));

                        var compensation = FormatCompensation(j);
                        if (!string.IsNullOrWhiteSpace(compensation))
                        {
                            description = string.IsNullOrWhiteSpace(description)
                                ? compensation
                                : $"{description}\n\nCompensation: {compensation}";
                        }

                        results.Add(new ScrapedJob
                        {
                            SourceId = SOURCE_ID,
                            ExternalId = GetText(j, "id"),
                            Title = title,
                            Company = companyName,
                            Location = location,
                            Description = description,
                            JobUrl = GetText(j, "jobUrl") ?? GetText(j, "applyUrl"),
                            IsRemote = isRemote,
                            DatePosted = ParseDate(
                                GetText(j, "publishedDate") ??
                                GetText(j, "publishedAt") ??
                                GetText(j, "createdAt") ??
                                GetText(j, "updatedAt")),
                            JobType = GetText(j, "employmentType"),
                            Category = BuildCategory(j)
                        });
                        added++;
                    }

                    Console.WriteLine($"[Ashby] {slug} -> {added} US/remote jobs");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Ashby] {slug} error: {ex.Message}");
                }

                await Task.Delay(800);
            }

            return results;
        }

        private static async Task<string> FetchJsonAsync(string url, string slug)
        {
            const int maxAttempts = 2;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    return await _http.GetStringAsync(url);
                }
                catch (TaskCanceledException) when (attempt < maxAttempts)
                {
                    Console.WriteLine($"[Ashby] {slug} -> timeout, retrying once");
                    await Task.Delay(2000);
                }
            }

            return await _http.GetStringAsync(url);
        }

        private static string GetLocation(JsonElement job)
        {
            var primary = GetLocationText(job, "location");
            var secondary = GetLocationList(job, "secondaryLocations");

            return string.Join(", ",
                new[] { primary }
                    .Concat(secondary)
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Distinct(StringComparer.OrdinalIgnoreCase));
        }

        private static string GetLocationText(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var value)) return "";

            if (value.ValueKind == JsonValueKind.String)
                return value.GetString() ?? "";

            if (value.ValueKind == JsonValueKind.Object)
                return GetText(value, "name") ?? GetText(value, "location") ?? "";

            return "";
        }

        private static IEnumerable<string> GetLocationList(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var value) ||
                value.ValueKind != JsonValueKind.Array)
                yield break;

            foreach (var item in value.EnumerateArray())
            {
                var text = item.ValueKind == JsonValueKind.String
                    ? item.GetString()
                    : item.ValueKind == JsonValueKind.Object
                        ? GetText(item, "name") ?? GetText(item, "location")
                        : null;

                if (!string.IsNullOrWhiteSpace(text))
                    yield return text;
            }
        }

        private static string BuildCategory(JsonElement job)
        {
            var values = new[]
            {
                GetText(job, "department"),
                GetText(job, "team")
            }
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase);

            return string.Join(" / ", values);
        }

        private static string FormatCompensation(JsonElement job)
        {
            if (!job.TryGetProperty("compensation", out var comp) ||
                comp.ValueKind == JsonValueKind.Null ||
                comp.ValueKind == JsonValueKind.Undefined)
                return null;

            if (comp.ValueKind == JsonValueKind.String)
                return comp.GetString();

            var text = GetText(comp, "compensationTierSummary")
                    ?? GetText(comp, "scrapeableCompensationSalarySummary")
                    ?? GetText(comp, "summary")
                    ?? GetText(comp, "description")
                    ?? GetText(comp, "display");

            if (!string.IsNullOrWhiteSpace(text)) return text;

            try
            {
                return comp.GetRawText();
            }
            catch
            {
                return null;
            }
        }

        private static string GetText(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var value)) return null;

            return value.ValueKind switch
            {
                JsonValueKind.String => value.GetString(),
                JsonValueKind.Number => value.ToString(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => null
            };
        }

        private static bool? GetBool(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var value)) return null;

            return value.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => null
            };
        }

        private static DateTime? ParseDate(string raw)
        {
            return DateTime.TryParse(raw, out var dt) ? dt : (DateTime?)null;
        }

        private static string StripHtml(string html)
        {
            if (string.IsNullOrEmpty(html)) return string.Empty;
            var text = HtmlStripPattern.Replace(html, " ");
            text = System.Net.WebUtility.HtmlDecode(text);
            return MultiSpacePattern.Replace(text, " ").Trim();
        }
    }
}
