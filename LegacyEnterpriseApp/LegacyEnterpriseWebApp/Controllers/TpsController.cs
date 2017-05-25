using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace LegacyEnterpriseWebApp.Controllers
{
    [RoutePrefix("api/tps")]
    public class TpsController : ApiController
    {
        [HttpPost]
        [Route("{name}")]
        public async Task<IHttpActionResult> Post(string name)
        {
            using (HttpClient client = new HttpClient())
            {
//                HttpResponseMessage apiResult = await client.PostAsync($"http://localhost:2232/api/reports/{name}", null);
                HttpResponseMessage apiResult = await client.PostAsync($"http://legacyenterprisedataapp:8080/api/reports/{name}", null);
                HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK);
                string result = await apiResult.Content.ReadAsStringAsync();

                return this.Json(new
                {
                    name = name,
                    id = result
                });
            }
        }
    }
}
