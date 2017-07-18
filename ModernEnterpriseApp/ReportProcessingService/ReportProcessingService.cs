using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ReportProcessingService.Models;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ReportProcessingService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class ReportProcessingService : StatefulService
    {
        internal static readonly Uri ProcessingQueueName = new Uri("store://processingQueue");
        internal static readonly Uri StatusDictionaryName = new Uri("store://statusDictionary");

        private const string cpuMetricName = "CPU";
        private const string memoryMetricName = "MemoryMB";
        private const string processingCapacityMetricName = "ProcessingCapacity";

        private readonly ProcessPerformanceCounter cpuCounter;
        private readonly ProcessPerformanceCounter memoryCounter;
        private readonly int processingMultiplier;
        private readonly Random random = new Random();
        private readonly ReportContext reportContext;
        

        public ReportProcessingService(StatefulServiceContext context)
            : base(context)
        {
            this.reportContext = new ReportContext(this.Context.ServiceName.Segments.Last());
            this.processingMultiplier = BitConverter.ToInt32(context.InitializationData, 0);
            this.cpuCounter = new ProcessPerformanceCounter("% Processor Time");
            this.memoryCounter = new ProcessPerformanceCounter("Working Set - Private");
            this.StateManager.StateManagerChanged += StateManager_StateManagerChanged;
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
            // get baseline perf counter samples
            GetCpuCounterValue(this.cpuCounter);
            GetMemoryMbCounterValue(this.memoryCounter);

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
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Creating"));
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Evaluating"));
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Reticulating"));

                    for (int i = 0; i < processingMultiplier * 10; ++i)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await processQueue.EnqueueAsync(tx, new ReportProcessingStep($"Processing step {i}"));
                    }

                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Sanitizing"));
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Mystery Step"));
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Finalizing"));
                    await processQueue.EnqueueAsync(tx, new ReportProcessingStep("Complete"));

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

                    ServiceEventSource.Current.ServiceMessage(
                       this.Context,
                       $"Processing step {currentProcessingStep.Name} complete.");

                }
                catch (ProcessPerformanceCounterException ppce)
                {
                    ServiceEventSource.Current.ServiceMessage(this.Context, ppce.Message);
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
        
        
        private void StateManager_StateManagerChanged(object sender, NotifyStateManagerChangedEventArgs e)
        {
            NotifyStateManagerSingleEntityChangedEventArgs operation = e as NotifyStateManagerSingleEntityChangedEventArgs;

            if (operation == null)
            {
                return;
            }

            if (operation.Action == NotifyStateManagerChangedAction.Add)
            {
                if (operation.ReliableState.Name == StatusDictionaryName)
                {
                    IReliableDictionary<string, ReportStatus> dictionary = operation.ReliableState as IReliableDictionary<string, ReportStatus>;

                    if (dictionary != null)
                    {
                        dictionary.DictionaryChanged += StatusDictionary_DictionaryChanged;

                        ServiceEventSource.Current.ServiceMessage(
                            this.Context,
                            $"Dictionary added. {sender.ToString()}");
                    }
                }
            }
        }

        private void StatusDictionary_DictionaryChanged(object sender, NotifyDictionaryChangedEventArgs<string, ReportStatus> e)
        { 
            int cpuMetricValue = GetCpuCounterValue(cpuCounter);
            int memoryMetricValue = GetMemoryMbCounterValue(memoryCounter);

            this.Partition.ReportLoad(new LoadMetric[]
            {
                new LoadMetric(processingCapacityMetricName, processingMultiplier),
                new LoadMetric(cpuMetricName,cpuMetricValue),
                new LoadMetric(memoryMetricName, memoryMetricValue),
            });

            ServiceEventSource.Current.ServiceMessage(
                this.Context,
                $"CPU reported: {cpuMetricValue}%. Memory reported: {memoryMetricValue} MB");

        }
        
        private static int GetCpuCounterValue(ProcessPerformanceCounter cpuCounter)
        {
            return (int)Math.Round(cpuCounter.NextValue() / Environment.ProcessorCount);
        }

        private static int GetMemoryMbCounterValue(ProcessPerformanceCounter memoryCounter)
        {
            return (int)Math.Round(memoryCounter.NextValue() / 1048576);
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

            TimeSpan ioDelay = TimeSpan.FromMilliseconds(this.random.Next(100, 200) / this.processingMultiplier);
            int randomLength = this.random.Next(1000, 2000) * this.processingMultiplier;
            string randomJunk = "";

            for (int i = 0; i < (10 * this.processingMultiplier); ++i)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // pointless inefficient string processing to drive up CPU and memory use
                randomJunk += new string(Enumerable.Repeat(chars, randomLength).Select(s => s[this.random.Next(s.Length)]).ToArray());
                
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