using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System.Fabric;
using Microsoft.AspNetCore.Hosting;
using System.Fabric.Query;
using System.Fabric.Description;

namespace ReportControllerService.Controllers
{
    [Route("api/[controller]")]
    public class ReportsController : Controller
    {
        private const string ReportProcessingServiceTypeName = "ReportProcessingServiceType";

        private readonly TimeSpan operationTimeout = TimeSpan.FromSeconds(20);
        private readonly FabricClient fabricClient;
        private readonly IApplicationLifetime appLifetime;
        private readonly StatelessServiceContext serviceContext;


        public ReportsController(StatelessServiceContext serviceContext, FabricClient fabricClient, IApplicationLifetime appLifetime)
        {
            this.serviceContext = serviceContext;
            this.fabricClient = fabricClient;
            this.appLifetime = appLifetime;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            ServiceList services = 
                await this.fabricClient.QueryManager.GetServiceListAsync(new Uri(this.serviceContext.CodePackageActivationContext.ApplicationName));

            return this.Ok(
                services
                    .Where(x => x.ServiceTypeName == ReportProcessingServiceTypeName)
                    .Select(
                        x => new
                        {
                            name = x.ServiceName.ToString(),
                            status = x.ServiceStatus.ToString(),
                            version = x.ServiceManifestVersion,
                            health = x.HealthState.ToString()
                        }));
        }

        [HttpPost]
        [Route("{reportName}")]
        public async Task<IActionResult> Post(string reportName)
        {
            // Now create the data service in the new application instance.
            StatefulServiceDescription dataServiceDescription = new StatefulServiceDescription()
            {
                ApplicationName = new Uri(this.serviceContext.CodePackageActivationContext.ApplicationName),
                HasPersistedState = true,
                MinReplicaSetSize = 3,
                TargetReplicaSetSize = 3,
                PartitionSchemeDescription = new SingletonPartitionSchemeDescription(),
                ServiceName = this.GetServiceName(reportName),
                ServiceTypeName = ReportProcessingServiceTypeName,
                ServicePackageActivationMode = ServicePackageActivationMode.ExclusiveProcess
            };

            try
            {
                await this.fabricClient.ServiceManager.CreateServiceAsync(dataServiceDescription, this.operationTimeout, this.appLifetime.ApplicationStopping);

                return this.Ok();
            }
            catch (FabricElementAlreadyExistsException)
            {
                return new ContentResult() { StatusCode = 400, Content = $"Service for report '{reportName}' already exists." };
            }
        }

        [HttpDelete]
        [Route("{reportName}")]
        public async Task<IActionResult> Delete(string reportName)
        {
            try
            {
                await this.fabricClient.ServiceManager.DeleteServiceAsync(
                    new DeleteServiceDescription(this.GetServiceName(reportName)),
                    this.operationTimeout,
                    this.appLifetime.ApplicationStopping);
            }
            catch (FabricElementNotFoundException)
            {
                // service doesn't exist; nothing to delete
            }

            return this.Ok();
        }

        private Uri GetServiceName(string reportName)
        {
            return new Uri($"{this.serviceContext.CodePackageActivationContext.ApplicationName}/TpsReports/Processing/{reportName}");
        }
    }
}
