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

namespace ReportProcessingService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class ReportProcessingService : StatefulService
    {
        internal const string ProcessingQueueName = "processingQueue";
        internal const string StatusDictionaryName = "statusDictionary";

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
            IReliableConcurrentQueue<ReportProcessingStep> processQueue
                = await this.StateManager.GetOrAddAsync<IReliableConcurrentQueue<ReportProcessingStep>>(ProcessingQueueName);

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

                    await statusDictionary.AddAsync(tx, this.reportContext.Name, new ReportStatus(0, "Not started."));
                }

                await tx.CommitAsync();
            }

            // start processing and checkpoint between each step so we don't lose any progress in the event of a fail-over
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    using (ITransaction tx = this.StateManager.CreateTransaction())
                    {
                        ConditionalValue<ReportProcessingStep> dequeueResult = await processQueue.TryDequeueAsync(tx, cancellationToken);

                        if (!dequeueResult.HasValue)
                        {
                            // all done!
                            break;
                        }

                        ReportProcessingStep currentProcessingStep = dequeueResult.Value;
                        
                        ServiceEventSource.Current.ServiceMessage(
                            this.Context,
                            $"Processing step: {currentProcessingStep.Name}");

                        // This takes a shared lock rather than an update lock
                        // because this is the only place the row is written to.
                        // If there were other writers, then this should be an update lock.
                        ConditionalValue<ReportStatus> dictionaryGetResult = 
                            await statusDictionary.TryGetValueAsync(tx, this.reportContext.Name, LockMode.Default);
                        
                        ReportStatus currentStatus = dictionaryGetResult.Value;
                        ReportStatus newStatus = await this.ProcessReport(currentStatus, currentProcessingStep, cancellationToken);

                        await statusDictionary.SetAsync(tx, this.reportContext.Name, newStatus);
                        
                        await tx.CommitAsync();
                    }
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
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            }


            ServiceEventSource.Current.ServiceMessage(
                this.Context,
                $"Processing complete!");
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
            await Task.Delay(TimeSpan.FromSeconds(new Random().Next(5, 15)), cancellationToken);

            return new ReportStatus(currentStatus.Step + 1, currentProcessingStep.Name);
        }
    }
}