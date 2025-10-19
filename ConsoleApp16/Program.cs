using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

class Program
{
    private static readonly HttpClient Http = CreateHttp();
    private static readonly object ConsoleLock = new();

    private const int MaxConcurrency = 8;
    private const int Retries = 2;
    private const int RetryDelayMs = 800;

    static async Task Main()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        var baseDir = AppContext.BaseDirectory;
        var linksPath = Path.Combine(baseDir, "links.txt");
        var keywordsPath = Path.Combine(baseDir, "keywords.txt");
        var outDir = Path.Combine(baseDir, "pages");
        Directory.CreateDirectory(outDir);

        if (!File.Exists(linksPath))
        {
            Console.WriteLine($"File links.txt not found: {linksPath}");
            return;
        }

        var urls = File.ReadAllLines(linksPath, Encoding.UTF8)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct()
            .ToList();

        var keywords = File.Exists(keywordsPath)
            ? File.ReadAllLines(keywordsPath, Encoding.UTF8)
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray()
            : Array.Empty<string>();

        Console.WriteLine($"Links found: {urls.Count}");
        Console.WriteLine($"Keywords loaded: {keywords.Length}");
        Console.WriteLine($"Links file: {(File.Exists(linksPath) ? linksPath : "missing")}");
        Console.WriteLine($"Keywords file: {(File.Exists(keywordsPath) ? keywordsPath : "missing")}\n");

        if (urls.Count == 0) return;

        var limiter = new SemaphoreSlim(MaxConcurrency);
        var rows = new ConcurrentBag<Row>();

        var tasks = urls.Select(async (url, i) =>
        {
            await limiter.WaitAsync();
            try
            {
                var (html, bytes, status) = await FetchWithRetries(url, Retries, RetryDelayMs);

                var title = ExtractTitle(html);
                var fileName = BuildFileName(i, url, title);
                var htmlPath = Path.Combine(outDir, fileName);

                await File.WriteAllTextAsync(htmlPath, html, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

                var text = ExtractVisibleText(html);
                int words = CountWords(text);

                var kwMap = CountKeywords(text, keywords);
                int kwTotal = kwMap.Values.Sum();

                var stats = new
                {
                    url,
                    title,
                    file = fileName,
                    bytes,
                    words,
                    keywords_total = kwTotal,
                    keywords_breakdown = kwMap
                };

                var statsPath = Path.Combine(outDir, Path.GetFileNameWithoutExtension(fileName) + ".stats.json");
                await File.WriteAllTextAsync(
                    statsPath,
                    JsonSerializer.Serialize(stats, new JsonSerializerOptions { WriteIndented = true }),
                    new UTF8Encoding(false)
                );

                lock (ConsoleLock)
                {
                    Console.WriteLine($"Processed: {url}");
                    Console.WriteLine($"file: {fileName}");
                    Console.WriteLine($"size: {bytes:N0} bytes");
                    Console.WriteLine($"words: {words}");
                    Console.WriteLine($"keywords: {kwTotal}\n");
                }

                rows.Add(new Row(url, status, fileName, bytes, title, words, kwTotal, null));
            }
            catch (HttpRequestException ex)
            {
                lock (ConsoleLock)
                    Console.WriteLine($"Failed to fetch: {url} -> {ex.Message}");
                rows.Add(new Row(url, ex.StatusCode ?? 0, "", 0, "", 0, 0, ex.Message));
            }
            catch (Exception ex)
            {
                lock (ConsoleLock)
                    Console.WriteLine($"Error processing: {url} -> {ex.Message}");
                rows.Add(new Row(url, 0, "", 0, "", 0, 0, ex.Message));
            }
            finally
            {
                limiter.Release();
            }
        }).ToArray();

        await Task.WhenAll(tasks);

        var manifest = Path.Combine(outDir, "manifest.csv");
        await WriteCsv(manifest, rows.OrderBy(r => r.FileName).ToList());
        Console.WriteLine($"\nFinished. Output folder: {outDir}\nManifest: {manifest}");
    }

    private record Row(
        string Url,
        HttpStatusCode Status,
        string FileName,
        long Bytes,
        string Title,
        int Words,
        int KeywordsTotal,
        string? Error
    );

    private static HttpClient CreateHttp()
    {
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate | DecompressionMethods.Brotli,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            ConnectTimeout = TimeSpan.FromSeconds(10)
        };

        var client = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(25)
        };

        client.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36");
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/html"));
        client.DefaultRequestHeaders.AcceptCharset.ParseAdd("utf-8");
        return client;
    }

    private static async Task<(string Html, long Bytes, HttpStatusCode Status)> FetchWithRetries(string url, int retries, int delayMs)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                using var resp = await Http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
                var status = resp.StatusCode;
                resp.EnsureSuccessStatusCode();

                var raw = await resp.Content.ReadAsByteArrayAsync();
                var html = DecodeHtml(raw, resp);
                return (html, raw.LongLength, status);
            }
            catch when (attempt < retries)
            {
                await Task.Delay(delayMs);
            }
        }
    }

    private static string DecodeHtml(byte[] bytes, HttpResponseMessage resp)
    {
        string? charset = resp.Content.Headers.ContentType?.CharSet;

        if (string.IsNullOrWhiteSpace(charset))
        {
            var head = Encoding.ASCII.GetString(bytes, 0, Math.Min(4096, bytes.Length));
            var m = Regex.Match(head, @"charset\s*=\s*[""']?(?<enc>[\w\-]+)", RegexOptions.IgnoreCase);
            if (m.Success) charset = m.Groups["enc"].Value;
        }

        charset ??= "utf-8";
        try { return Encoding.GetEncoding(charset).GetString(bytes); }
        catch { return Encoding.UTF8.GetString(bytes); }
    }

    private static string ExtractTitle(string html)
    {
        var m = Regex.Match(html, @"<title>\s*(.*?)\s*</title>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var title = m.Success ? m.Groups[1].Value : "";
        return Regex.Replace(title, @"\s+", " ").Trim();
    }

    private static string ExtractVisibleText(string html)
    {
        var noScripts = Regex.Replace(html, @"<script[\s\S]*?</script>", "", RegexOptions.IgnoreCase);
        var noStyles = Regex.Replace(noScripts, @"<style[\s\S]*?</style>", "", RegexOptions.IgnoreCase);
        var noTags = Regex.Replace(noStyles, @"<[^>]+>", " ");
        noTags = WebUtility.HtmlDecode(noTags);
        return Regex.Replace(noTags, @"\s+", " ").Trim();
    }

    private static int CountWords(string text) =>
        Regex.Matches(text, @"[\p{L}\p{Nd}]+").Count;

    private static Dictionary<string, int> CountKeywords(string text, string[] keywords)
    {
        var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (keywords.Length == 0 || string.IsNullOrWhiteSpace(text)) return result;

        var lower = text.ToLowerInvariant();

        var bag = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (Match m in Regex.Matches(lower, @"[\p{L}\p{Nd}]+"))
        {
            var w = m.Value;
            bag.TryGetValue(w, out var c);
            bag[w] = c + 1;
        }

        foreach (var raw in keywords)
        {
            var k = raw.Trim();
            if (k.Length == 0) continue;

            int count;
            if (!k.Contains(' '))
            {
                bag.TryGetValue(k.ToLowerInvariant(), out count);
            }
            else
            {
                var pattern = $@"(?<![\p{{L}}\p{{Nd}}]){Regex.Escape(k.ToLowerInvariant())}(?![\p{{L}}\p{{Nd}}])";
                count = Regex.Matches(lower, pattern).Count;
            }

            if (count > 0) result[k] = count;
        }

        return result;
    }

    private static string BuildFileName(int index, string url, string title)
    {
        var uri = new Uri(url);
        var host = Sanitize(uri.Host.Replace("www.", ""));

        var tail = string.Join("_",
            uri.AbsolutePath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries)
               .Reverse().Take(2));
        if (string.IsNullOrWhiteSpace(tail)) tail = "index";

        var stub = string.IsNullOrWhiteSpace(title) ? tail : title;
        var name = $"{(index + 1):D3}_{host}_{Sanitize(stub)}";
        if (name.Length > 120) name = name[..120];
        return name + ".html";
    }

    private static string Sanitize(string s)
    {
        s = s.Replace(':', '-');
        var bad = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s) sb.Append(bad.Contains(ch) ? '_' : ch);
        return sb.ToString();
    }

    private static async Task WriteCsv(string path, IList<Row> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine("url,status,file,bytes,words,keywords_total,title,error");
        foreach (var r in rows)
        {
            sb.AppendLine(string.Join(",",
                Csv(r.Url),
                (int)r.Status,
                Csv(r.FileName),
                r.Bytes,
                r.Words,
                r.KeywordsTotal,
                Csv(r.Title),
                Csv(r.Error ?? "")));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), new UTF8Encoding(false));

        static string Csv(string s) => "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
