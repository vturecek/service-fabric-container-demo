using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace ReportProcessingService
{
    public class ProcessPerformanceCounter : IDisposable
    {
        private PerformanceCounter counter;
        private readonly string counterName;

        public ProcessPerformanceCounter(string counterName)
        {
            this.counterName = counterName;
        }

        public float NextValue()
        {
            for (int i = 0; i < 3; ++i)
            {
                try
                {
                    return this.GetCounter().NextValue();
                }
                catch (InvalidOperationException)
                {
                    ResetCounter();
                }
            }

            return 0.0f;
        }

        private PerformanceCounter GetCounter()
        {
            if (counter == null)
            {
                counter = new PerformanceCounter("Process", this.counterName, GetProcessInstanceName());
            }

            return counter;
        }

        private void ResetCounter()
        {
            this.Dispose();
        }

        private static string GetProcessInstanceName()
        {
            Process p = Process.GetCurrentProcess();

            PerformanceCounterCategory cat = new PerformanceCounterCategory("Process");

            string[] instances = cat.GetInstanceNames();
            foreach (string instance in instances)
            {

                using (PerformanceCounter cnt = new PerformanceCounter("Process",
                     "ID Process", instance, true))
                {
                    int val = (int)cnt.RawValue;
                    if (val == p.Id)
                    {
                        return instance;
                    }
                }
            }

            throw new InvalidOperationException();
        }

        public void Dispose()
        {
            if (this.counter != null)
            {
                this.counter.Dispose();
                this.counter = null;
            }
        }
    }
}
