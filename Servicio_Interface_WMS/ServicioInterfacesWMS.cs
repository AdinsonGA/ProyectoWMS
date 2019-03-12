using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
//using System.Text;
using System.Threading.Tasks;
using System.Timers;
using CapaInterface;

//using WinSCP;
//using System.Data.SqlClient;
//using System.Configuration;

   
namespace CapaServicio
{
    public partial class ServicioInterfaceWMS : ServiceBase
    {

        // Timer para Prescripciones
        Timer tmservicio = null;
        Int32 wenproceso = 0;

        // Timer para O/C


        public ServicioInterfaceWMS()
        {
            InitializeComponent();

            // Prescripciones
            tmservicio = new Timer(30000);
            tmservicio.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed);   
            
            // O/C

            
        }


        //****************************************************************************
        void tmpServicio_Elapsed(object sender, ElapsedEventArgs e)
        {
            // verificar si el servicio se esta ejecutando
            if (wenproceso == 0)
            {
                wenproceso = 1;
                Prescripcion oprescrip = new Prescripcion();
                oprescrip.Genera_Interface_Prescripcion();
                wenproceso = 0;
            }
        }

        protected override void OnStart(string[] args)
        {
            tmservicio.Start();
        }

        protected override void OnStop()
        {
            tmservicio.Stop();
        }


        internal void TestStartupAndStop()
        {
            string[] arg = new string[] { }; ;
            this.OnStart(arg);
            Console.ReadLine();
            this.OnStop();
        }
    }
}
