using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ReportProcessingService.Models
{
    public class ReportContext
    {
        public ReportContext(string name)
        {
            this.Name = name;
        }

        public string Name { get; private set; }
    }
}
