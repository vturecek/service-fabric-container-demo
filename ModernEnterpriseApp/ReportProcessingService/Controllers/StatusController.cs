using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using ReportProcessingService.Models;
using System.Fabric;
using System.Threading;

namespace ReportProcessingService.Controllers
{
    [Route("api/[controller]")]
    public class StatusController : Controller
    {
        private readonly IReliableStateManager stateManager;
        private readonly ReportContext reportContext;

        public StatusController(IReliableStateManager stateManager, ReportContext reportContext)
        {
            this.stateManager = stateManager;
            this.reportContext = reportContext;
        }

        // GET api/status
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            string statusValue = "Nothing to show yet.";
            long remainingValue = -1;

            try
            {
                ConditionalValue<IReliableDictionary<string, ReportStatus>> tryGetDictionaryResult =
                    await this.stateManager.TryGetAsync<IReliableDictionary<string, ReportStatus>>(ReportProcessingService.StatusDictionaryName);

                if (tryGetDictionaryResult.HasValue)
                {
                    IReliableDictionary<string, ReportStatus> dictionary = tryGetDictionaryResult.Value;

                    using (ITransaction tx = this.stateManager.CreateTransaction())
                    {
                        ConditionalValue<ReportStatus> getResult = await dictionary.TryGetValueAsync(tx, this.reportContext.Name);

                        if (getResult.HasValue)
                        {
                            statusValue = getResult.Value.Status;
                        }
                    }
                }

                ConditionalValue<IReliableConcurrentQueue<ReportProcessingStep>> tryGetQueueResult =
                    await this.stateManager.TryGetAsync<IReliableConcurrentQueue<ReportProcessingStep>>(ReportProcessingService.ProcessingQueueName);

                if (tryGetQueueResult.HasValue)
                {
                    IReliableConcurrentQueue<ReportProcessingStep> queue = tryGetQueueResult.Value;

                    remainingValue = queue.Count;
                }

                return this.Json(new { status = statusValue, remaining = remainingValue });
            }
            catch (FabricNotPrimaryException)
            {
                return new ContentResult { StatusCode = 410, Content = "The primary replica has moved. Please re-resolve the service." };
            }
            catch (Exception ex) when (ex is FabricTransientException || ex is TimeoutException)
            {
                return new ContentResult { StatusCode = 503, Content = "The service was unable to process the request. Please try again." };
            }
        }

    }
}
