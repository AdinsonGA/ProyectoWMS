using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace CapaServicio
{
    static class Program
    {
        /// <summary>
        /// Punto de entrada principal para la aplicación.
        /// </summary>
        static void Main()
        {

            if (Environment.UserInteractive)
            {
                ServicioInterfaceWMS service1 = new ServicioInterfaceWMS();
                service1.TestStartupAndStop();
            }
            else
            {
              
            
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new ServicioInterfaceWMS()
            };
            ServiceBase.Run(ServicesToRun);
            }
        }
    }
}
