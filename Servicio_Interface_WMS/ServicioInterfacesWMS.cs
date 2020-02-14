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
using CapaDatos;
using System.Configuration;
//using System.Threading;

//using WinSCP;
//using System.Data.SqlClient;
//using System.Configuration;


namespace CapaServicio
{
    public partial class ServicioInterfaceWMS : ServiceBase
    {

        // Timer para Prescripciones
        Timer tmservicio_Prescripciones = null;
        Int32 wenproceso_Prescripciones = 0;

        // Timer para O/C  (J Cochon)
        Timer tmservicio_Purchase = null;
        Int32 wenproceso_Purchase = 0;

        // Timer para ASN O/C (J Cochon)
        Timer tmservicio_ASN_Purchase = null;
        Int32 wenproceso_ASN_Purchase = 0;

        // Timer para Leer TXT del FTP (J Cochon)
        Timer tmservicio_Leer_FTP = null;
        Int32 wenproceso_Leer_FTP = 0;

        // Timer para ASN Devoluciones Tiendas
        Timer tmservicio_ASN_Devol = null;
        Int32 wenproceso_ASN_Devol = 0;

        //timer para order HDR,order DTL --pedido carrito
        Timer tmservicio_HDR_DTL = null;
        Int32 wenproceso_HDR_DTL = 0;

        //timer para order HDR,order DTL --pedido catalogo
        Timer tmservicio_HDR_DTL_catalogo = null;
        Int32 wenproceso_HDR_DTL_catalogo = 0;

        // timer para actualizar lista con tabla tabgen.dbf
        Timer tmactulista = null;

        //timer stock WMS
        Timer tmservicio_stock_WMS = null;
        Int32 wenproceso_stock_wms = 0;

        //timer NC ASN carrito
        Timer tmservicio_NC_carrito = null;
        Int32 wenproceso_NC_carrito = 0;

        //timer NC ASN carrito
        Timer tmservicio_NC_catalogo = null;
        Int32 wenproceso_NC_catalogo = 0;

        public ServicioInterfaceWMS()
        {
            double minutos = 60000;

            InitializeComponent();
            // Timer para actualizar lista de TABGEN.DBF
            tmactulista = new Timer(1 * minutos); // initial start after 1 second
            tmactulista.Elapsed += new ElapsedEventHandler(tmactulista_Elapsed);

            // Prescripciones
            tmservicio_Prescripciones = new Timer(5 * minutos);
            tmservicio_Prescripciones.Elapsed += new ElapsedEventHandler(tmpServicio_Prescripciones_Elapsed);

            // Purchase            
            tmservicio_Purchase = new Timer(30 * minutos);
            tmservicio_Purchase.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_Purchase);

            // ASN Purchase
            tmservicio_ASN_Purchase = new Timer(5 * minutos); //5
            tmservicio_ASN_Purchase.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Purchase);

            // Leer archivos del Ftp
            tmservicio_Leer_FTP = new Timer(2 * minutos);
            tmservicio_Leer_FTP.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_Leer_FTP);

            // ASN Devoluciones de Tiendas
            tmservicio_ASN_Devol = new Timer(10 * minutos);
            tmservicio_ASN_Devol.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Devol);

            // order HDR, DTL carrito
            tmservicio_HDR_DTL = new Timer(5 * minutos); //5
            tmservicio_HDR_DTL.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_HDR_DTL_Carrito);

            // order HDR, DTL catalogo
            tmservicio_HDR_DTL_catalogo = new Timer(5 * minutos); //5
            tmservicio_HDR_DTL_catalogo.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_HDR_DTL_catalogo);

            // stock
            tmservicio_stock_WMS = new Timer(1 * minutos);
            tmservicio_stock_WMS.Elapsed += new ElapsedEventHandler(tmpServicioStock_WMS);

            //NC carrito
            tmservicio_NC_carrito = new Timer(10 * minutos);
            tmservicio_NC_carrito.Elapsed += new ElapsedEventHandler(tmpServicio_NC_carrito);

            //NC catalogo
            tmservicio_NC_catalogo = new Timer(10 * minutos);
            tmservicio_NC_catalogo.Elapsed += new ElapsedEventHandler(tmpServicio_NC_catalogo);

        }

        //****************************************************************************

        void tmactulista_Elapsed(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {

                Timer timer = (Timer)sender;
                timer.Interval = 60 * 60 * 1000;      // Change the interval to whatever (1 hora)
                DatosGenerales.Llena_CDxAlm();
                LogUtil.Graba_Log("SERVICIO", ConfigurationManager.AppSettings["M011"].ToString(), false, "");

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log("SERVICIO", " ERROR: " + ex.ToString(), true, "");
                //throw ex;
            }

        }

        void tmpServicio_Prescripciones_Elapsed(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {

                if (ConfigurationManager.AppSettings["processPrescrip"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_Prescripciones == 0)
                    {
                        wenproceso_Prescripciones = 1;
                        Prescripcion oprescrip = new Prescripcion();
                        oprescrip.Genera_Interface_Prescripcion(1); // 1=retail
                        oprescrip.Genera_Interface_Prescripcion(2); // 2=no retail
                        wenproceso_Prescripciones = 0;

                    }
                }
            }
            catch (Exception ex)
            {
                wenproceso_Prescripciones = 0;
            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_Purchase(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["processPurchase"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_Purchase == 0)
                    {
                        wenproceso_Purchase = 1;
                        Purchase opurchase = new Purchase();
                        opurchase.Genera_Interface_Purchase();
                        wenproceso_Purchase = 0;

                    }
                }
            }
            catch (Exception ex)
            {
                wenproceso_Purchase = 0;

            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_ASN_Purchase(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_ASN_Purchase"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_ASN_Purchase == 0)
                    {

                        wenproceso_ASN_Purchase = 1;
                        Asn_Purchase oasn_purchase = new Asn_Purchase();
                        oasn_purchase.Genera_Interface_Asn_Purchase();
                        wenproceso_ASN_Purchase = 0;


                    }
                }
            }
            catch (Exception ex)
            {
                wenproceso_ASN_Purchase = 0;
            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_Leer_FTP(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["processLeerFTP"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_Leer_FTP == 0)
                    {

                        wenproceso_Leer_FTP = 1;
                        Leer_Ftp oLeer_Ftp = new Leer_Ftp();
                        oLeer_Ftp.Genera_Interface_Lectura();
                        wenproceso_Leer_FTP = 0;
                    }
                }
            }
            catch (Exception ex)
            {
                wenproceso_Leer_FTP = 0;
            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_HDR_DTL_Carrito(object sender, ElapsedEventArgs e)  //carrito de compras
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_HDR_HDL_carrito"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_HDR_DTL == 0)
                    {

                        wenproceso_HDR_DTL = 1;
                        Pedidos_Carrito ohdr_dtl = new Pedidos_Carrito();
                        bool exito = ohdr_dtl.Genera_Interface_Carrito_Maestro(); // maestros de carrito

                        if (exito == false)
                        {
                            ohdr_dtl.Genera_Interface_Carrito_Pedido(); // orden de pedidos de carrito
                        }
                        
                        wenproceso_HDR_DTL = 0;

                    }
                }
            }
            catch (Exception ex)
            {
                //ex.Message.ToString();
                wenproceso_HDR_DTL = 0;
            }

        }

        //****************************************************************************
        void tmpServicio_Elapsed_ASN_Devol(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_ASN_Devol"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_ASN_Devol == 0)
                    {

                        wenproceso_ASN_Devol = 1;
                        ASN_Devolucion odev = new ASN_Devolucion();
                        odev.Genera_Interface_ASN_Devolucion();
                        wenproceso_ASN_Devol = 0;
                    }
                }
            }
            catch (Exception)
            {

                wenproceso_ASN_Devol = 0;
            }

        }

        //****************************************************************************
        void tmpServicio_Elapsed_HDR_DTL_catalogo(object sender, ElapsedEventArgs e)

        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_HDR_HDL_catalogo"] == "1")
                {
                    // verificar si el servicio se esta ejecutando

                    if (wenproceso_HDR_DTL_catalogo == 0)
                    {

                        wenproceso_HDR_DTL_catalogo = 1;
                        Pedidos_Catalogo ohdr_dtl_catalogo = new Pedidos_Catalogo();
                        bool exito = ohdr_dtl_catalogo.Genera_Interface_Catalogo_Maestro(); // maestros de catalogo

                        if (exito == false)
                        {
                            ohdr_dtl_catalogo.Genera_Interface_Catalogo_Pedido(); //orden de pedido de catalogo
                        }
                        wenproceso_HDR_DTL_catalogo = 0;

                    }
                }
            }
            catch (Exception)
            {
                wenproceso_HDR_DTL_catalogo = 0;
            }

        }



        void tmpServicioStock_WMS(object sender, ElapsedEventArgs e)
        {

            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion

            //string cadena_conexion = Conexion.conexion;

            //LogUtil.Graba_Log("PRUEBA", "FLAG ACTUALIZADO : " + cadena_conexion, true, "");

            try
            {

                if (ConfigurationManager.AppSettings["process_stock_WMS"] == "1")
                {
                    if (Convert.ToDateTime(DateTime.Now.Hour.ToString() + ":" + DateTime.Now.Minute.ToString()).ToString("HH:mm") == ConfigurationManager.AppSettings["hora_stock_1"].ToString() ||  //verificar las horas configuradas para la actualizacion de stock
                        Convert.ToDateTime(DateTime.Now.Hour.ToString() + ":" + DateTime.Now.Minute.ToString()).ToString("HH:mm") == ConfigurationManager.AppSettings["hora_stock_2"].ToString() ||
                        Convert.ToDateTime(DateTime.Now.Hour.ToString() + ":" + DateTime.Now.Minute.ToString()).ToString("HH:mm") == ConfigurationManager.AppSettings["hora_stock_3"].ToString())

                    {
                        if (wenproceso_stock_wms == 0)
                        {

                            wenproceso_stock_wms = 1;
                            Stock objStock = new Stock();
                            objStock.LeerStock();
                            wenproceso_stock_wms = 0;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ex.Message.ToString();

                wenproceso_stock_wms = 0;
            }

        }

        //nuevo ASN

        void tmpServicio_NC_carrito(object sender, ElapsedEventArgs e)  //carrito de compras
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_NC_carrito"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_NC_carrito == 0)
                    {
                        wenproceso_NC_carrito = 1;
                        //Pedidos_Carrito ohdr_dtl = new Pedidos_Carrito();
                        ASN_Carrito oNC_carrito = new ASN_Carrito();
                        //ohdr_dtl.Genera_Interface_OrdDesp();
                        oNC_carrito.GenerarInterfaceNC_carrito();
                        wenproceso_NC_carrito = 0;
                    }
                }
            }
            catch (Exception)
            {
                wenproceso_NC_carrito = 0;
            }

        }

        void tmpServicio_NC_catalogo(object sender, ElapsedEventArgs e)
        {
            ConfigurationManager.RefreshSection("appSettings"); // Primero actualiza seccion
            try
            {
                if (ConfigurationManager.AppSettings["process_NC_catalogo"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_NC_catalogo == 0)
                    {
                        wenproceso_NC_catalogo = 1;
                        //Pedidos_Carrito ohdr_dtl = new Pedidos_Carrito();
                        ASN_Catalogo oNC_catalogo = new ASN_Catalogo();
                        //ohdr_dtl.Genera_Interface_OrdDesp();

                        oNC_catalogo.GeneraInterfaceNC_catalogo();
                        wenproceso_NC_catalogo = 0;
                    }
                }
            }
            catch (Exception)
            {
                wenproceso_NC_catalogo = 0;
            }
        }

        protected override void OnStart(string[] args)
        {
            //Conexion.Mapea_red();
            //DatosGenerales.Llena_CDxAlm();

            tmactulista.Start();
            tmservicio_Prescripciones.Start();
            tmservicio_Purchase.Start();
            tmservicio_ASN_Purchase.Start();
            tmservicio_Leer_FTP.Start();
            tmservicio_ASN_Devol.Start();
            ///*interface hdr/dtl*/
            tmservicio_HDR_DTL_catalogo.Start();
            tmservicio_HDR_DTL.Start();
            tmservicio_stock_WMS.Start();
            //nuevo asn
            tmservicio_NC_carrito.Start();
            tmservicio_NC_catalogo.Start();

        }

        protected override void OnStop()
        {
            tmactulista.Stop();
            tmservicio_Prescripciones.Stop();
            tmservicio_Purchase.Stop();
            tmservicio_ASN_Purchase.Stop();
            tmservicio_Leer_FTP.Stop();
            tmservicio_ASN_Devol.Stop();

            ///*interface hdr/dtl*/
            tmservicio_HDR_DTL_catalogo.Stop();
            tmservicio_HDR_DTL.Stop();
            tmservicio_stock_WMS.Stop();
            //nuevo asn
            tmservicio_NC_carrito.Stop();
            tmservicio_NC_catalogo.Stop();
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
