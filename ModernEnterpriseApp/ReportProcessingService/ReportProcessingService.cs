using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ReportProcessingService.Models;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Fabric;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Net.Http;
using System.Diagnostics;
using System.Collections.Specialized;

namespace ReportProcessingService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class ReportProcessingService : StatefulService
    {
        internal const string ProcessingQueueName = "processingQueue";
        internal const string StatusDictionaryName = "statusDictionary";

        private const string cpuMetricName = "CPU";
        private const string memoryMetricName = "MemoryMB";

        private readonly int processingMultiplier;
        private readonly Random random = new Random();

        private static readonly IEnumerable<string> processingSteps = new ReadOnlyCollection<string>(new[]
        {
            "Creating",
            "Evaluating",
            "Calculating",
            "Reticulating",
            "Guesstimating",
            "Synergizing",
            "Finalizing",
            "Complete"
        });

        private readonly ReportContext reportContext;

        public ReportProcessingService(StatefulServiceContext context)
            : base(context)
        {
            this.reportContext = new ReportContext(this.Context.ServiceName.Segments.Last());
            this.processingMultiplier = BitConverter.ToInt32(context.InitializationData, 0);
        }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[]
            {
                new ServiceReplicaListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                            .UseKestrel()
                            .ConfigureServices(
                                services => services
                                    .AddSingleton<ReportContext>(this.reportContext)
                                    .AddSingleton<IReliableStateManager>(this.StateManager)
                                    .AddSingleton<StatefulServiceContext>(serviceContext))
                            .UseContentRoot(Directory.GetCurrentDirectory())
                            .UseStartup<Startup>()
                            .UseApplicationInsights()
                            .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseUniqueServiceUrl)
                            .UseUrls(url)
                            .Build();
                    }))
            };
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            using (ProcessPerformanceCounter cpuCounter = new ProcessPerformanceCounter("% Processor Time"))
            {
                using (ProcessPerformanceCounter memoryCounter = new ProcessPerformanceCounter("Working Set - Private"))
                {
                    // get a baseline metric
                    this.Partition.ReportLoad(new LoadMetric[]
                    {
                        new LoadMetric(cpuMetricName, (int)Math.Round(cpuCounter.NextValue())),
                        new LoadMetric(memoryMetricName, (int)Math.Round(memoryCounter.NextValue())),
                    });

                    IReliableQueue<ReportProcessingStep> processQueue
                        = await this.StateManager.GetOrAddAsync<IReliableQueue<ReportProcessingStep>>(ProcessingQueueName);

                    IReliableDictionary<string, ReportStatus> statusDictionary
                        = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, ReportStatus>>(StatusDictionaryName);

                    // queue up all the processing steps and create an initial processing status if one doesn't exist already
                    using (ITransaction tx = this.StateManager.CreateTransaction())
                    {
                        ConditionalValue<ReportStatus> tryGetResult
                            = await statusDictionary.TryGetValueAsync(tx, this.reportContext.Name, LockMode.Update);

                        if (!tryGetResult.HasValue)
                        {
                            foreach (string processingStep in processingSteps)
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                await processQueue.EnqueueAsync(tx, new ReportProcessingStep(processingStep));
                            }

                            await statusDictionary.AddAsync(tx, this.reportContext.Name, new ReportStatus(0, "Not started.", String.Empty));
                        }

                        await tx.CommitAsync();
                    }

                    // start processing and checkpoint between each step so we don't lose any progress in the event of a fail-over
                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            // Get the next step from the queue with a peek.
                            // This keeps the item on the queue in case processing fails.
                            ConditionalValue<ReportProcessingStep> dequeueResult;
                            using (ITransaction tx = this.StateManager.CreateTransaction())
                            {
                                dequeueResult = await processQueue.TryPeekAsync(tx, LockMode.Default);
                            }

                            if (!dequeueResult.HasValue)
                            {
                                // all done!
                                break;
                            }

                            ReportProcessingStep currentProcessingStep = dequeueResult.Value;

                            ServiceEventSource.Current.ServiceMessage(
                                this.Context,
                                $"Processing step: {currentProcessingStep.Name}");

                            // Get the current processing step
                            ConditionalValue<ReportStatus> dictionaryGetResult;
                            using (ITransaction tx = this.StateManager.CreateTransaction())
                            {
                                dictionaryGetResult = await statusDictionary.TryGetValueAsync(tx, this.reportContext.Name, LockMode.Default);
                            }

                            // Perform the next processing step. 
                            // This is potentially a long-running operation, therefore it is not executed within a transaction.
                            ReportStatus currentStatus = dictionaryGetResult.Value;
                            ReportStatus newStatus = await this.ProcessReport(currentStatus, currentProcessingStep, cancellationToken);

                            // Once processing is done, save the results and dequeue the processing step.
                            using (ITransaction tx = this.StateManager.CreateTransaction())
                            {
                                dequeueResult = await processQueue.TryDequeueAsync(tx);

                                await statusDictionary.SetAsync(tx, this.reportContext.Name, newStatus);

                                await tx.CommitAsync();
                            }

                            // report metrics after each iteration.
                            // this uses perforance counters, which will report the average since the last sample was taken in the previous iteration.

                            this.Partition.ReportLoad(new LoadMetric[]
                            {
                                new LoadMetric(cpuMetricName, (int)Math.Round(cpuCounter.NextValue())),
                                new LoadMetric(memoryMetricName, (int)Math.Round(memoryCounter.NextValue())),
                            });
                        }
                        catch (TimeoutException)
                        {
                            // transient error. Retry.
                            ServiceEventSource.Current.ServiceMessage(this.Context, "TimeoutException in RunAsync.");
                        }
                        catch (FabricTransientException fte)
                        {
                            // transient error. Retry.
                            ServiceEventSource.Current.ServiceMessage(this.Context, "FabricTransientException in RunAsync: {0}", fte.Message);
                        }
                        catch (FabricNotPrimaryException)
                        {
                            // not primary any more, time to quit.
                            return;
                        }
                        catch (FabricNotReadableException)
                        {
                            // retry or wait until not primary
                            ServiceEventSource.Current.ServiceMessage(this.Context, "FabricNotReadableException in RunAsync.");
                        }
                        catch (Exception ex)
                        {
                            // all other exceptions: log and re-throw.
                            ServiceEventSource.Current.ServiceMessage(this.Context, "Exception in RunAsync: {0}", ex.Message);

                            throw;
                        }

                        // delay between each to step to prevent starving other processing service instances.
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    }


                    ServiceEventSource.Current.ServiceMessage(

                        this.Context,
                            $"Processing complete!");
                }
            }
        }


        /// <summary>
        /// "Processes" a report
        /// </summary>
        /// <param name="currentStatus"></param>
        /// <param name="currentProcessingStep"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async Task<ReportStatus> ProcessReport(ReportStatus currentStatus, ReportProcessingStep currentProcessingStep, CancellationToken cancellationToken)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            TimeSpan ioDelay = TimeSpan.FromMilliseconds(this.random.Next(1000, 2000) / this.processingMultiplier);
            int randomLength = this.random.Next(100, 1000) * this.processingMultiplier;

            string randomJunk = "";
            for (int i = 0; i < (100 * this.processingMultiplier); ++i)
            {
                // pointless inefficient string processing to drive up CPU and memory use
                for (int j = 0; j < 100 * this.processingMultiplier; ++j)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    randomJunk += new string(Enumerable.Repeat(chars, randomLength).Select(s => s[this.random.Next(s.Length)]).ToArray());
                }

                int numberOfAs = 0;
                for (int j = 0; j < randomJunk.Length; ++j)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (randomJunk[j] == 'A')
                    {
                        ++numberOfAs;
                    }
                }

                await Task.Delay(ioDelay, cancellationToken);
            }

            return new ReportStatus(currentStatus.Step + 1, currentProcessingStep.Name, randomJunk);
        }
    }
}