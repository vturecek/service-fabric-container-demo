using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace LegacyEnterpriseWebApp.Controllers
{
    [RoutePrefix("api/reports")]
    public class ReportsController : ApiController
    {
        [HttpPost]
        [Route("{name}")]
        public Task<HttpResponseMessage> Post(string name)
        {
            HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(Guid.NewGuid().ToString());
            
            return Task.FromResult(response);
        }
    }
}
