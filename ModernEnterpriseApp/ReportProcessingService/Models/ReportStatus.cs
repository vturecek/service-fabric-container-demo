using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ReportProcessingService.Models
{
    [DataContract]
    internal class ReportStatus
    {
        public ReportStatus(int step, int remaining, string status, string result)
        {
            this.Step = step;
            this.Remaining = remaining;
            this.Status = status;
            this.Result = result;
        }
        
        [DataMember]
        public int Step { get; private set; }

        [DataMember]
        public int Remaining { get; private set; }

        [DataMember]
        public string Status { get; private set; }
        
        [DataMember]
        public string Result { get; private set; }
    }
}
