using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ReportProcessingService.Models
{

    [DataContract]
    internal class ReportProcessingStep
    {
        public ReportProcessingStep(string name)
        {
            this.Name = name;
        }

        [DataMember]
        public string Name { get; private set; }
    }
}
