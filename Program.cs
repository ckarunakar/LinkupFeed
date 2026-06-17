using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Xml;

namespace LinkupFeed
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            try
            {
                await RunAsync(args);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Fatal] {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine(ex);
                Environment.ExitCode = 1;
            }
        }

        private static async System.Threading.Tasks.Task RunAsync(string[] args)
        {
            // await RemoteJobsScraping();

            if (args != null && args.Length > 0 &&
                string.Equals(args[0], "taleo", StringComparison.OrdinalIgnoreCase))
            {
                await RunTaleoOnlyAsync();
                return;
            }

            if (args != null && args.Length > 0 &&
                string.Equals(args[0], "successfactors", StringComparison.OrdinalIgnoreCase))
            {
                await RunSuccessFactorsOnlyAsync();
                return;
            }

            if (args != null && args.Length > 0 &&
                string.Equals(args[0], "lever", StringComparison.OrdinalIgnoreCase))
            {
                await RunLeverOnlyAsync();
                return;
            }

            if (args != null && args.Length > 0 &&
                string.Equals(args[0], "ashby", StringComparison.OrdinalIgnoreCase))
            {
                await RunAshbyOnlyAsync(
                    HasArg(args, "--write-db") && !HasArg(args, "--dry-run") && !HasArg(args, "--no-write-db"),
                    GetIntArg(args, "--limit") ?? GetIntArg(args, "--max-inserts"),
                    GetStringArg(args, "--company") ?? GetStringArg(args, "--slug"));
                return;
            }

            if (args == null || args.Length == 0 ||
                string.Equals(args[0], "all", StringComparison.OrdinalIgnoreCase))
            {
                await RunAllScrapersWithDedupeAsync(
                    !HasArg(args, "--dry-run") && !HasArg(args, "--no-write-db"),
                    GetIntArg(args, "--limit") ?? GetIntArg(args, "--max-inserts"));
                return;
            }

            SqlConnection Sqlconn = new SqlConnection("Data source=209.59.189.133\\ITJOBCAFESERVER,1435;Initial Catalog=feeds;User Id=itjobcafe;Pwd=Chand@789!");
            Sqlconn.Open();
            Console.WriteLine($"[DB] Connection opened: state={Sqlconn.State}");

            try
            {
                var jobs = await new FreeJobScraperOrchestrator().RunAllAsync();

                // US-only filter
                int freeBefore = jobs.Count;
                jobs = jobs.Where(j => UsLocationFilter.IsUs(j.Location)).ToList();
                Console.WriteLine($"[Free scrapers] US filter: {freeBefore} -> {jobs.Count}");

                // IT-only filter
                int freeItBefore = jobs.Count;
                jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[Free scrapers] IT filter: {freeItBefore} -> {jobs.Count}");

                // Plug into your existing ETL:
                int freeOk = 0, freeFail = 0;
                foreach (var job in jobs)
                {
                    try
                    {
                        job.ExternalId = Guid.NewGuid().ToString();
                        //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                        freeOk++;
                    }
                    catch (Exception jobEx)
                    {
                        freeFail++;
                        Console.WriteLine($"[Insert-Free] FAILED ExternalId={job.ExternalId} Source={job.SourceId} — {jobEx.Message}");
                    }
                }
                Console.WriteLine($"[Free scrapers] Attempted={jobs.Count} Inserted={freeOk} Failed={freeFail}");

                //--workday jobs
                var workdayjobs = await new WorkdayPreprocesser().ProcessAllTenantsAsync();

                // US-only filter
                int wdBefore = workdayjobs.Count;
                workdayjobs = workdayjobs.Where(j => UsLocationFilter.IsUs(j.Location)).ToList();
                Console.WriteLine($"[Workday] US filter: {wdBefore} -> {workdayjobs.Count}");

                int wdItBefore = workdayjobs.Count;
                workdayjobs = workdayjobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[Workday] IT filter: {wdItBefore} -> {workdayjobs.Count}");

                int wdOk = 0, wdFail = 0;
                foreach (var job in workdayjobs)
                {
                    try
                    {
                        job.ExternalId = Guid.NewGuid().ToString();
                        //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                        wdOk++;
                    }
                    catch (Exception jobEx)
                    {
                        wdFail++;
                        Console.WriteLine($"[Insert-Workday] FAILED ExternalId={job.ExternalId} Company={job.Company} — {jobEx.Message}");
                    }
                }
                Console.WriteLine($"[Workday] Attempted={workdayjobs.Count} Inserted={wdOk} Failed={wdFail}");

                //--SuccessFactors jobs
                var sfJobs = await new SuccessFactorsScraper().FetchJobsAsync();

                int sfBefore = sfJobs.Count;
                sfJobs = sfJobs.Where(j => UsLocationFilter.IsUs(j.Location)).ToList();
                Console.WriteLine($"[SuccessFactors] US filter: {sfBefore} -> {sfJobs.Count}");

                int sfItBefore = sfJobs.Count;
                sfJobs = sfJobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[SuccessFactors] IT filter: {sfItBefore} -> {sfJobs.Count}");

                int sfOk = 0, sfFail = 0;
                foreach (var job in sfJobs)
                {
                    try
                    {
                        job.ExternalId = Guid.NewGuid().ToString();
                        //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                        sfOk++;
                    }
                    catch (Exception jobEx)
                    {
                        sfFail++;
                        Console.WriteLine($"[Insert-SuccessFactors] FAILED ExternalId={job.ExternalId} Company={job.Company} — {jobEx.Message}");
                    }
                }
                Console.WriteLine($"[SuccessFactors] Attempted={sfJobs.Count} Inserted={sfOk} Failed={sfFail}");

                //--Lever jobs
                var leverJobs = await new LeverScraper().FetchJobsAsync();

                int lvBefore = leverJobs.Count;
                leverJobs = leverJobs.Where(j => UsLocationFilter.IsUs(j.Location) || j.IsRemote == true).ToList();
                Console.WriteLine($"[Lever] US filter: {lvBefore} -> {leverJobs.Count}");

                int lvItBefore = leverJobs.Count;
                leverJobs = leverJobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[Lever] IT filter: {lvItBefore} -> {leverJobs.Count}");

                int lvOk = 0, lvFail = 0;
                foreach (var job in leverJobs)
                {
                    try
                    {
                        job.ExternalId = Guid.NewGuid().ToString();
                        //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                        lvOk++;
                    }
                    catch (Exception jobEx)
                    {
                        lvFail++;
                        Console.WriteLine($"[Insert-Lever] FAILED ExternalId={job.ExternalId} Company={job.Company} — {jobEx.Message}");
                    }
                }
                Console.WriteLine($"[Lever] Attempted={leverJobs.Count} Inserted={lvOk} Failed={lvFail}");

                //--Ashby jobs
                var ashbyJobs = await new AshbyScraper().FetchJobsAsync();

                int ashbyBefore = ashbyJobs.Count;
                ashbyJobs = ashbyJobs.Where(j => UsLocationFilter.IsUs(j.Location) || j.IsRemote == true).ToList();
                Console.WriteLine($"[Ashby] US filter: {ashbyBefore} -> {ashbyJobs.Count}");

                int ashbyItBefore = ashbyJobs.Count;
                ashbyJobs = ashbyJobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[Ashby] IT filter: {ashbyItBefore} -> {ashbyJobs.Count}");

                int ashbyOk = 0, ashbyFail = 0;
                foreach (var job in ashbyJobs)
                {
                    try
                    {
                        job.ExternalId = Guid.NewGuid().ToString();
                        //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                        ashbyOk++;
                    }
                    catch (Exception jobEx)
                    {
                        ashbyFail++;
                        Console.WriteLine($"[Insert-Ashby] FAILED ExternalId={job.ExternalId} Company={job.Company} - {jobEx.Message}");
                    }
                }
                Console.WriteLine($"[Ashby] Attempted={ashbyJobs.Count} Inserted={ashbyOk} Failed={ashbyFail}");
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if (Sqlconn != null)
                {
                    if (Sqlconn.State.ToString() == "Open")
                    {
                        Sqlconn.Close();
                    }
                }
            }

        }

        private static async System.Threading.Tasks.Task RunAllScrapersWithDedupeAsync(bool writeToDatabase, int? insertLimit)
        {
            var connectionString = Environment.GetEnvironmentVariable(JobDatabaseSync.ConnectionStringEnvVar);
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    $"Set {JobDatabaseSync.ConnectionStringEnvVar} before running the combined scraper.");
            }

            var allJobs = new List<ScrapedJob>();

            await AddFilteredJobsAsync(
                allJobs,
                "Free scrapers",
                () => new FreeJobScraperOrchestrator().RunAllAsync(),
                includeRemoteWithoutUsLocation: false);

            await AddFilteredJobsAsync(
                allJobs,
                "Workday",
                () => new WorkdayPreprocesser().ProcessAllTenantsAsync(),
                includeRemoteWithoutUsLocation: false);

            await AddFilteredJobsAsync(
                allJobs,
                "SuccessFactors",
                () => new SuccessFactorsScraper().FetchJobsAsync(),
                includeRemoteWithoutUsLocation: false);

            await AddFilteredJobsAsync(
                allJobs,
                "Lever",
                () => new LeverScraper().FetchJobsAsync(),
                includeRemoteWithoutUsLocation: true);

            await AddFilteredJobsAsync(
                allJobs,
                "Ashby",
                () => new AshbyScraper().FetchJobsAsync(),
                includeRemoteWithoutUsLocation: true);

            await AddFilteredJobsAsync(
                allJobs,
                "Greenhouse",
                () => new GreenhouseAtsScraper().FetchJobsAsync(),
                includeRemoteWithoutUsLocation: true);

            await AddFilteredJobsAsync(
                allJobs,
                "Taleo",
                () => new TaleoScraper().FetchJobsAsync(),
                includeRemoteWithoutUsLocation: false);

            Console.WriteLine($"[All] Combined filtered jobs before dedupe: {allJobs.Count}");

            Console.WriteLine("[All] Loading existing database keys...");
            var existing = JobDatabaseSync.LoadExistingKeys(connectionString);
            Console.WriteLine(
                $"[All] Existing DB urls={existing.Urls.Count}, references={existing.References.Count}, fingerprints={existing.Fingerprints.Count}");

            var deduped = JobDatabaseSync.RemoveDuplicates(allJobs, existing);

            Console.WriteLine($"[All] Skipped DB duplicates: {deduped.DbDuplicates}");
            Console.WriteLine($"[All] Skipped batch duplicates: {deduped.BatchDuplicates}");
            Console.WriteLine($"[All] New jobs after dedupe: {deduped.NewJobs.Count}");

            if (writeToDatabase)
            {
                var jobsToInsert = insertLimit.HasValue
                    ? deduped.NewJobs.Take(insertLimit.Value).ToList()
                    : deduped.NewJobs;

                if (insertLimit.HasValue)
                {
                    Console.WriteLine($"[All] Insert limit enabled: {jobsToInsert.Count} of {deduped.NewJobs.Count} deduped jobs will be inserted.");
                }

                Console.WriteLine("[All] WRITE MODE ENABLED. Inserting deduped jobs into database...");
                JobDatabaseSync.InsertJobs(connectionString, jobsToInsert, "[All]");
            }
            else
            {
                Console.WriteLine("[All] Dry run only; no database writes. Remove --dry-run/--no-write-db to insert deduped jobs.");
            }
        }

        private static async System.Threading.Tasks.Task AddFilteredJobsAsync(
            List<ScrapedJob> allJobs,
            string label,
            Func<System.Threading.Tasks.Task<List<ScrapedJob>>> fetch,
            bool includeRemoteWithoutUsLocation)
        {
            try
            {
                Console.WriteLine($"[{label}] Starting scraper...");
                var jobs = await fetch();
                Console.WriteLine($"[{label}] Raw jobs: {jobs.Count}");

                int locationBefore = jobs.Count;
                jobs = jobs
                    .Where(j => UsLocationFilter.IsUs(j.Location) || (includeRemoteWithoutUsLocation && j.IsRemote))
                    .ToList();
                Console.WriteLine($"[{label}] US/remote filter: {locationBefore} -> {jobs.Count}");

                int itBefore = jobs.Count;
                jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
                Console.WriteLine($"[{label}] IT filter: {itBefore} -> {jobs.Count}");

                allJobs.AddRange(jobs);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{label}] FAILED: {ex.Message}");
            }
        }

        private static bool HasArg(string[] args, string name)
        {
            return args != null && args.Any(a => string.Equals(a, name, StringComparison.OrdinalIgnoreCase));
        }

        private static int? GetIntArg(string[] args, string name)
        {
            if (args == null) return null;

            for (int i = 0; i < args.Length; i++)
            {
                if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase) &&
                    i + 1 < args.Length &&
                    int.TryParse(args[i + 1], out var value) &&
                    value >= 0)
                {
                    return value;
                }
            }

            return null;
        }

        private static string GetStringArg(string[] args, string name)
        {
            if (args == null) return null;

            for (int i = 0; i < args.Length; i++)
            {
                if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase) &&
                    i + 1 < args.Length)
                {
                    return args[i + 1];
                }
            }

            return null;
        }

        private static async System.Threading.Tasks.Task RunSuccessFactorsOnlyAsync()
        {
            Console.WriteLine("[SF-Only] Starting scraper...");
            var jobs = await new SuccessFactorsScraper().FetchJobsAsync();
            Console.WriteLine($"[SF-Only] Scraper returned {jobs.Count} jobs (pre-US-filter).");

            int before = jobs.Count;
            jobs = jobs.Where(j => UsLocationFilter.IsUs(j.Location)).ToList();
            Console.WriteLine($"[SF-Only] US filter: {before} -> {jobs.Count}");

            int itBefore = jobs.Count;
            jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
            Console.WriteLine($"[SF-Only] IT filter: {itBefore} -> {jobs.Count}");

            foreach (var j in jobs.Take(10))
                Console.WriteLine($"  - [{j.Company}] {j.Title} | {j.Location} | {j.JobUrl}");

            using var Sqlconn = new SqlConnection("Data source=209.59.189.133\\ITJOBCAFESERVER,1435;Initial Catalog=feeds;User Id=itjobcafe;Pwd=Chand@789!");
            Sqlconn.Open();
            Console.WriteLine($"[DB] Connection opened: state={Sqlconn.State}");

            int ok = 0, fail = 0;
            foreach (var job in jobs)
            {
                try
                {
                    job.ExternalId = Guid.NewGuid().ToString();
                    //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                    ok++;
                }
                catch (Exception ex)
                {
                    fail++;
                    Console.WriteLine($"[Insert-SF] FAILED ExternalId={job.ExternalId} — {ex.Message}");
                }
            }
            Console.WriteLine($"[SF-Only] Attempted={jobs.Count} Inserted={ok} Failed={fail}");
        }

        private static async System.Threading.Tasks.Task RunLeverOnlyAsync()
        {
            Console.WriteLine("[Lever-Only] Starting scraper...");
            var jobs = await new LeverScraper().FetchJobsAsync();
            Console.WriteLine($"[Lever-Only] Scraper returned {jobs.Count} jobs (pre-US-filter).");

            int before = jobs.Count;
            jobs = jobs.Where(j => UsLocationFilter.IsUs(j.Location) || j.IsRemote == true).ToList();
            Console.WriteLine($"[Lever-Only] US filter: {before} -> {jobs.Count}");

            int itBefore = jobs.Count;
            jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
            Console.WriteLine($"[Lever-Only] IT filter: {itBefore} -> {jobs.Count}");

            foreach (var j in jobs.Take(10))
                Console.WriteLine($"  - [{j.Company}] {j.Title} | {j.Location} | {j.JobUrl}");

            using var Sqlconn = new SqlConnection("Data source=209.59.189.133\\ITJOBCAFESERVER,1435;Initial Catalog=feeds;User Id=itjobcafe;Pwd=Chand@789!");
            Sqlconn.Open();
            Console.WriteLine($"[DB] Connection opened: state={Sqlconn.State}");

            int ok = 0, fail = 0;
            foreach (var job in jobs)
            {
                try
                {
                    job.ExternalId = Guid.NewGuid().ToString();
                    //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                    ok++;
                }
                catch (Exception ex)
                {
                    fail++;
                    Console.WriteLine($"[Insert-Lever] FAILED ExternalId={job.ExternalId} — {ex.Message}");
                }
            }
            Console.WriteLine($"[Lever-Only] Attempted={jobs.Count} Inserted={ok} Failed={fail}");
        }

        private static async System.Threading.Tasks.Task RunAshbyOnlyAsync(bool writeToDatabase, int? insertLimit, string onlyCompany)
        {
            Console.WriteLine("[Ashby-Only] Starting scraper...");
            var jobs = await new AshbyScraper().FetchJobsAsync(onlyCompany);
            Console.WriteLine($"[Ashby-Only] Scraper returned {jobs.Count} jobs (pre-US-filter).");

            int before = jobs.Count;
            jobs = jobs.Where(j => UsLocationFilter.IsUs(j.Location) || j.IsRemote == true).ToList();
            Console.WriteLine($"[Ashby-Only] US filter: {before} -> {jobs.Count}");

            int itBefore = jobs.Count;
            jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
            Console.WriteLine($"[Ashby-Only] IT filter: {itBefore} -> {jobs.Count}");

            foreach (var j in jobs.Take(10))
                Console.WriteLine($"  - [{j.Company}] {j.Title} | {j.Location} | {j.JobUrl}");

            if (!writeToDatabase)
            {
                Console.WriteLine("[Ashby-Only] Inspection mode only; add --write-db to insert deduped jobs.");
                return;
            }

            var connectionString = Environment.GetEnvironmentVariable(JobDatabaseSync.ConnectionStringEnvVar);
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    $"Set {JobDatabaseSync.ConnectionStringEnvVar} before writing Ashby jobs to the database.");
            }

            Console.WriteLine("[Ashby-Only] Loading existing database keys...");
            var existing = JobDatabaseSync.LoadExistingKeys(connectionString);
            Console.WriteLine(
                $"[Ashby-Only] Existing DB urls={existing.Urls.Count}, references={existing.References.Count}, fingerprints={existing.Fingerprints.Count}");

            var deduped = JobDatabaseSync.RemoveDuplicates(jobs, existing);

            Console.WriteLine($"[Ashby-Only] Skipped DB duplicates: {deduped.DbDuplicates}");
            Console.WriteLine($"[Ashby-Only] Skipped batch duplicates: {deduped.BatchDuplicates}");
            Console.WriteLine($"[Ashby-Only] New jobs after dedupe: {deduped.NewJobs.Count}");

            var jobsToInsert = insertLimit.HasValue
                ? deduped.NewJobs.Take(insertLimit.Value).ToList()
                : deduped.NewJobs;

            if (insertLimit.HasValue)
            {
                Console.WriteLine($"[Ashby-Only] Insert limit enabled: {jobsToInsert.Count} of {deduped.NewJobs.Count} deduped jobs will be inserted.");
            }

            Console.WriteLine("[Ashby-Only] WRITE MODE ENABLED. Inserting deduped jobs into database...");
            JobDatabaseSync.InsertJobs(connectionString, jobsToInsert, "[Ashby-Only]");
        }

        private static async System.Threading.Tasks.Task RunTaleoOnlyAsync()
        {
            Console.WriteLine("[Taleo-Only] Starting scraper...");
            var jobs = await new TaleoScraper().FetchJobsAsync();
            Console.WriteLine($"[Taleo-Only] Scraper returned {jobs.Count} jobs (pre-US-filter).");

            int before = jobs.Count;
            jobs = jobs.Where(j => UsLocationFilter.IsUs(j.Location)).ToList();
            Console.WriteLine($"[Taleo-Only] US filter: {before} -> {jobs.Count}");

            int itBefore = jobs.Count;
            jobs = jobs.Where(j => ItJobFilter.IsIt(j.Title, j.Category)).ToList();
            Console.WriteLine($"[Taleo-Only] IT filter: {itBefore} -> {jobs.Count}");

            foreach (var j in jobs.Take(10))
                Console.WriteLine($"  - [{j.Company}] {j.Title} | {j.Location} | {j.JobUrl}");

            using var Sqlconn = new SqlConnection("Data source=209.59.189.133\\ITJOBCAFESERVER,1435;Initial Catalog=feeds;User Id=itjobcafe;Pwd=Chand@789!");
            Sqlconn.Open();
            Console.WriteLine($"[DB] Connection opened: state={Sqlconn.State}");

            int ok = 0, fail = 0;
            foreach (var job in jobs)
            {
                try
                {
                    job.ExternalId = Guid.NewGuid().ToString();
                    //FeedInsert.Post_Jobs(job.Location, job.Title, LocationSplitter.CityOf(job.Location), LocationSplitter.StateOf(job.Location), "", "USA", job.JobType, job.DatePosted.ToString(), job.ExternalId, job.Company, job.IsRemote == true ? 1 : 0, job.Category, "", job.JobUrl, job.Description, "", Sqlconn);
                    ok++;
                }
                catch (Exception ex)
                {
                    fail++;
                    Console.WriteLine($"[Insert-Taleo] FAILED ExternalId={job.ExternalId} — {ex.Message}");
                }
            }
            Console.WriteLine($"[Taleo-Only] Attempted={jobs.Count} Inserted={ok} Failed={fail}");
        }
    }
}
