using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WinSCP;
using CapaDatos;
using System.Data.OleDb;
//using System.Data.SqlTypes;

namespace CapaInterface
{

    public class Resultado
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }


    public class Prescripcion
    {

        //************** Envio de Prescripciones       
        string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string waction = "CREATE";
        int wdiasatras = 30;

        //************** Datatable global para guardar las prescripciones obtenidas
        DataTable dat_presccabRetail = null;
        DataTable dat_prescdetRetail = null;
        DataTable dat_presccabNoRetail = null;
        DataTable dat_prescdetNoRetail = null;

        //************** Files de texto
        //string nomfiltxt1 = $"ORH{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        //string nomfiltxt2 = $"ORD{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_Prescripcion()
        {
            bool exito = false;

            try
            {

                //var resultadoObtPresc = Obtiene_Prescrip();
                //if (!resultadoObtPres.Exito)
                //{
                //    //GraboLog
                //    //Cierro conexion
                //    return;
                //}

                //var resultadoGeneraFileTxt = Genera_FileTXT();
                //if (!resultadoGeneraFileTxt.Exito)
                //{
                //    //GraboLog
                //    //Cierro conexion
                //    return;
                //}


                LogHandle.Graba_Log("******************** INICIO PROCESO");

                if (Obtiene_Prescrip())
                {
                    if (Genera_FileTXT())
                    {
                        if (Envia_FTP())
                        {
                            if (Actualiza_Flag_Prescrip())
                            {
                                exito = true;
                            }
                        }
                    }

                }

                Archiva_TXT();

                if (exito)
                {
                    LogHandle.Graba_Log("SE PROCESO OK"); // OJO POR MIENTRAS
                }
                else
                {
                    LogHandle.Graba_Log("NO PROCESO NADA"); // OJO POR MIENTRAS
                }


            }
            catch (Exception ex)
            {
                LogHandle.Graba_Log("ERROR: " + ex.ToString());
            }
            finally
            {
                LogHandle.Graba_Log("******************** FIN PROCESO");
            }
        }
        //****************************************************************************



        /************** Actualiza_Flag_Prescrip
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Prescrip()
        {

            bool exito1 = false;
            bool exito2 = false;

            exito1 = Actualiza_Flag("500");
            exito2 = Actualiza_Flag("600");

            return (exito1 || exito2);

        }


        private bool Actualiza_Flag(string retail_noretail)
        {

            bool exito = false;
            string cade = "";
            var listaCade = new List<string>();

            DataTable dtaux = null;

            if (retail_noretail == "500")
            { dtaux = dat_presccabRetail; }
            else
            { dtaux = dat_presccabNoRetail; }

            // OJO FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

            if (dtaux != null && dtaux.Rows.Count > 0)
            {

                foreach (DataRow fila in dtaux.Rows)
                {
                    if (retail_noretail == "500")
                    {
                        cade += "'" + Convert.ToString(fila["cgud_gudis"]).Trim() + "',";
                    }
                    else
                    {
                        cade += "'" + Convert.ToString(fila["oc_nord"]).Trim() + "',";
                    }

                    // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
                    if (cade.Length > 900)
                    {
                        cade = cade.TrimEnd(',');
                        listaCade.Add(cade);
                        cade = "";
                    }
                }
                
                cade = cade.TrimEnd(',');
                listaCade.Add(cade);      


                // RETAIL
                using (OleDbConnection dbConn = new OleDbConnection(Conexion.Conn1))
                {
                    dbConn.Open();

                    //using (OleDbCommand cmd = dbConn.CreateCommand())
                    //{
                    //    cmd.CommandText = "=SYS(3055, 440)";
                    //    cmd.ExecuteNonQuery();
                    //}
                    //new System.Data.OleDb.OleDbCommand("SYS(3055, 2040)", dbConn).ExecuteNonQuery();

                    //cmd.CommandText = "SYS(3055, 2040)";
                    //cmd.ExecuteNonQuery();

                    //string valor = fila["Prescrip"].ToString();
                    //string sql_upd = "UPDATE FVPRESP SET PRE_RECNO=1 WHERE PRE_TIEND='" + fila["Pre_tiend"] + "' AND PRE_ARTIC='" + fila["Pre_artic"] + "' AND PRE_CALID='" + fila["Pre_calid"] + "' AND PRE_ARTIC='2811304' AND PRE_TIEND='50522'";

                    string sql_upd = "";

                    foreach (var caden in listaCade)
                    {
                        if (retail_noretail == "500")
                        {
                            sql_upd = "UPDATE SCCCGUD SET CGUD_FTXTD='X' WHERE cgud_gudis IN (" + caden + ")";
                        }
                        else
                        {
                            sql_upd = "UPDATE vmaoc SET oc_ftx='X' WHERE oc_nord IN (" + caden + ")";
                        }
                        System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
                        com_upd.ExecuteNonQuery();
                        int count = caden.Count(f => f == ',');
                        LogHandle.Graba_Log("UPDATE " + retail_noretail + " Docum: " + Convert.ToString(count+1));
                    }

                }

            }

            //// NO RETAIL

            //if (dat_presccabNoRetail != null && dat_presccabNoRetail.Rows.Count > 0)
            //{
            //    cade = "";

            //    foreach (DataRow fila in dat_presccabNoRetail.Rows)
            //    {
            //        //cade = cade + fila["oc_nord"].ToString().Trim() + ",";
            //        cade += "'" + Convert.ToString(fila["oc_nord"]).Trim() + "',";
            //    }

            //    cade = cade.TrimEnd(',');
            //    //cade = cade.Substring(0, cade.Length - 1);

            //    using (System.Data.OleDb.OleDbConnection dbConn = new System.Data.OleDb.OleDbConnection(Conexion.Conn2))
            //    {
            //        dbConn.Open();

            //        //string valor = fila["Prescrip"].ToString();
            //        //string sql_upd = "UPDATE FVPRESP SET PRE_RECNO=1 WHERE PRE_TIEND='" + fila["Pre_tiend"] + "' AND PRE_ARTIC='" + fila["Pre_artic"] + "' AND PRE_CALID='" + fila["Pre_calid"] + "' AND PRE_ARTIC='2811304' AND PRE_TIEND='50522'";
            //        string sql_upd = "UPDATE vmaoc SET oc_ftx='X' WHERE oc_nord IN (" + cade + ")";
            //        System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
            //        com_upd.ExecuteNonQuery();
            //        LogHandle.Graba_Log("UPDATE vmaoc");

            //    }
            //}

            exito = true;
            return exito;
        }




        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP()
        {
            bool exito1 = false;
            bool exito2 = false;

            //string NombreArchivo = Path.GetFileName(fileTXTc);
            //string logftp = DatosGenerales.rutaMain + "logftp.txt";

            exito1 = Send_FTP_WMS(fileTXTc, fileTXTc);
            exito2 = Send_FTP_WMS(fileTXTd, fileTXTd);


            //using (StreamWriter str = new StreamWriter(logftp, false, System.Text.Encoding.Default))
            //{

            //}

            if (exito1 && exito2) LogHandle.Graba_Log("ENVIA FTP OK "); else LogHandle.Graba_Log("ENVIA FTP ERROR ");

            return (exito1 && exito2);
        }


        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT()
        {

            bool exito1 = false;
            bool exito2 = false;

            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(DatosGenerales.rutaMain, "ORH_"+fechor);
            fileTXTd = Path.Combine(DatosGenerales.rutaMain, "ORD_"+fechor);

            // Eliminar archivos ORH, ORD.TXT
            try
            {
                var dir = new DirectoryInfo(DatosGenerales.rutaMain);
                foreach (var file in dir.EnumerateFiles("OR*.TXT"))
                {
                    file.Delete();
                }
            }
            catch
            {
                // omitido
            }

            //if (File.Exists(fileTXTc)) { File.Delete(fileTXTc); }
            //if (File.Exists(fileTXTd)) { File.Delete(fileTXTd); }

            exito1 = Genera_FileTXT_Retail();
            exito2 = Genera_FileTXT_NoRetail();

            return (exito1 || exito2);
        }


        private bool Genera_FileTXT_Retail()
        {

            if (dat_presccabRetail == null || dat_presccabRetail.Rows.Count == 0)
            { return false; }
          
            string delimited = "|";
            bool exito = false;
            var str = new StringBuilder();

            foreach (DataRow datarow in dat_presccabRetail.Rows)
            {
                str.Append(datarow["cgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                str.Append(wcodalm + delimited);                                  // Facility code
                str.Append(wcodcia + delimited);                                  // Cod Cia
                str.Append(datarow["cgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                str.Append(datarow["cgud_canal"].ToString() + datarow["cgud_almac"].ToString() + delimited);  // Order Type ejemplo: 5K
                str.Append(Convert.ToDateTime(datarow["cgud_femis"]).ToString("yyyyMMdd") + delimited);        // Fecha emision
                str.Append("" + delimited);                                      // exp_date
                str.Append(Convert.ToDateTime(datarow["cgud_femis"]).ToString("yyyyMMdd") + delimited);        // Fecha de entrega requerida
                str.Append(datarow["cgud_tndcl"].ToString() + delimited);        // dest_facility_code (cod tienda)
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(waction + delimited);                                 // action code
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["cgud_tndcl"].ToString() + delimited);       // Tienda 
                for (int i = 1; i <= 17; i++)
                { str.Append("" + delimited); };
                str.Append("" + delimited);                                     // Nro O/C cliente ??
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["cgud_caden"].ToString() + delimited);       // Cadena
                str.Append("" + delimited);
                str.Append(Convert.ToDateTime(datarow["cgud_femis"]).ToString("yyyyMMdd") + delimited);       // Fecha de entrega requerida  ???
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);                                     // Ruta despacho ??
                str.Append(datarow["cgud_almac"].ToString() + delimited);       // Almacen 
                str.Append(datarow["cgud_canal"].ToString() + delimited);       // 5 , 6 
                str.Append("" + delimited);                                     // RUC destinatario
                str.Append("\r\n");
            }

            if (File.Exists(fileTXTc)) { File.Delete(fileTXTc); }
            File.WriteAllText(fileTXTc, str.ToString());


            // DETALLE RETAIL
            int correlativo = 0;
            string keyitem = null;
            char cero = '0';
            string grupo = "";

            str = new StringBuilder();

            grupo = dat_prescdetRetail.Rows[0]["dgud_gudis"].ToString();

            foreach (DataRow datarow in dat_prescdetRetail.Rows)
            {
                // Resetear correlativo cuando cambia de grupo
                if (datarow["dgud_gudis"].ToString() != grupo)
                {
                    correlativo = 0;
                    grupo = datarow["dgud_gudis"].ToString();
                }
                                

                for (int xi = 0; xi < 12; xi++)
                {

                    string pad = xi.ToString().PadLeft(2, cero);
                    var value = datarow["dgud_med" + pad];
                    if (value != DBNull.Value)
                    {
                        int cant = Convert.ToInt32(value);
                        if (cant != 0)
                        {
                            correlativo += 1;

                            string pos = (xi + 1).ToString().PadLeft(2, cero);

                            // Evaluar si el articulo es prepack o solid
                            if (datarow["dgud_cpack"].ToString() == "00001")
                            { keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + datarow["dgud_cpack"].ToString() + DatosGenerales.CodRetail; }
                            else
                            { keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + pos + DatosGenerales.CodRetail; }
                            
                            str.Append(datarow["dgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                            str.Append(wcodalm + delimited);                                  // Facility code
                            str.Append(wcodcia + delimited);                                  // Cod Cia
                            str.Append(datarow["dgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                            str.Append(correlativo.ToString() + delimited);                  // Numero correlativo
                            str.Append(keyitem + delimited);                                 // Key item
                            for (int i = 1; i <= 10; i++)
                            { str.Append("" + delimited); };
                            str.Append(cant.ToString() + delimited);                         // Cantidad
                            str.Append("" + delimited);
                            str.Append(waction + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append(datarow["dgud_costo"].ToString() + delimited);        // Costo
                            str.Append("\r\n");
                        }
                    }
                }

            }

            if (File.Exists(fileTXTd)) { File.Delete(fileTXTd); }
            File.WriteAllText(fileTXTd, str.ToString());

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));
            return exito;

        }


        private bool Genera_FileTXT_NoRetail()
        {

            if (dat_presccabNoRetail == null || dat_presccabNoRetail.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            bool exito = false;
            var str = new StringBuilder();

            foreach (DataRow datarow in dat_presccabNoRetail.Rows)
            {
                str.Append(datarow["oc_nord"].ToString() + delimited);           // Numero de orden de despacho
                str.Append(wcodalm + delimited);                                  // Facility code
                str.Append(wcodcia + delimited);                                  // Cod Cia
                str.Append(datarow["oc_nord"].ToString() + delimited);        // Numero de orden de despacho
                str.Append(datarow["oc_canal"].ToString() + datarow["oc_almac"].ToString() + delimited);  // Order Type ejemplo: 5K
                str.Append(Convert.ToDateTime(datarow["oc_fecha"]).ToString("yyyyMMdd") + delimited);        // Fecha emision
                str.Append("" + delimited);                                      // exp_date
                str.Append(Convert.ToDateTime(datarow["oc_fecha"]).ToString("yyyyMMdd") + delimited);        // Fecha de entrega requerida
                str.Append(datarow["oc_client"].ToString() + delimited);        // dest_facility_code (cod cliente)
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(waction + delimited);                                 // action code
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["oc_client"].ToString() + delimited);        // Cliente 
                for (int i = 1; i <= 17; i++)
                { str.Append("" + delimited); };
                str.Append(datarow["oc_docref"].ToString() + delimited);        // Nro O/C cliente ??
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["oc_caden"].ToString() + delimited);         // Cadena
                str.Append("" + delimited);
                str.Append(Convert.ToDateTime(datarow["oc_fecha"]).ToString("yyyyMMdd") + delimited);       // Fecha de entrega requerida  ???
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);                                      // Ruta despacho ??
                str.Append(datarow["oc_almac"].ToString() + delimited);         // Almacen 
                str.Append(datarow["oc_canal"].ToString() + delimited);         // 5 , 6 
                str.Append(datarow["oc_ccli"].ToString() + delimited);          // RUC destinatario
                str.Append("\r\n");

            }


            using (StreamWriter filtxt = new StreamWriter(fileTXTc, true, System.Text.Encoding.Default))
            {
                filtxt.WriteLine(str.ToString());
            }


            // DETALLE NORETAIL
            int correlativo = 0;
            string keyitem = null;
            char cero = '0';
            string grupo = "";

            str = new StringBuilder();

            grupo = dat_prescdetNoRetail.Rows[0]["od_nord"].ToString();

            foreach (DataRow datarow in dat_prescdetNoRetail.Rows)
            {

                // Resetear correlativo cuando cambia de grupo
                if (datarow["od_nord"].ToString() != grupo)
                { 
                    correlativo = 0;
                    grupo = datarow["od_nord"].ToString();
                }
                
                for (int xi = 0; xi < 12; xi++)
                {

                    string pad = xi.ToString().PadLeft(2, cero);
                    var value = datarow["od_qo" + pad];
                    if (value != DBNull.Value)
                    {
                        int cant = Convert.ToInt32(value);
                        if (cant != 0)
                        {
                            correlativo += 1;

                            string pos = (xi + 1).ToString().PadLeft(2, cero);

                            // Evaluar si el articulo es prepack o solid
                            if (datarow["od_cpack"].ToString() == "00001")
                                keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + datarow["od_cpack"].ToString() + DatosGenerales.CodNoRetail;
                            else
                                keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + pos + DatosGenerales.CodNoRetail;

                            str.Append(datarow["od_nord"].ToString() + delimited);            // Numero de orden de despacho
                            str.Append(wcodalm + delimited);                                  // Facility code
                            str.Append(wcodcia + delimited);                                  // Cod Cia
                            str.Append(datarow["od_nord"].ToString() + delimited);            // Numero de orden de despacho
                            str.Append(correlativo.ToString() + delimited);                   // Numero correlativo
                            str.Append(keyitem + delimited);                                  // Key item
                            for (int i = 1; i <= 10; i++)
                            { str.Append("" + delimited); };
                            str.Append(cant.ToString() + delimited);                          // Cantidad
                            str.Append("" + delimited);
                            str.Append(waction + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append("" + delimited);
                            str.Append(datarow["od_costo"].ToString() + delimited);           // Costo
                            str.Append("\r\n");
                        }
                    }
                }
            }

            using (StreamWriter filtxt = new StreamWriter(fileTXTd, true, System.Text.Encoding.Default))
            {
                filtxt.WriteLine(str.ToString());
            }

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));
            return exito;
        }



        /************** Obtiene_Prescrip
        * Metodo que obtiene las prescripciones desde el Sis (dbf)
        ***************/

        private bool Obtiene_Prescrip2xxxxxxxxxxxx()
        {

            bool exito = false;
            dat_presccabRetail = null;
            dat_prescdetRetail = null;
            dat_presccabNoRetail = null;
            dat_prescdetNoRetail = null;

            using (System.Data.OleDb.OleDbConnection dbConn = new System.Data.OleDb.OleDbConnection("hola"))
            {
                dbConn.Open();

                try
                {

                    //-- Obtenemos prescripciones RETAIL Cabecera
                    string sql_prescripc = "SELECT cgud_gudis,cgud_tndcl,cgud_canal,cgud_caden,cgud_almac,cgud_femis FROM SCCCGUD WHERE EMPTY(CGUD_FTXTD) AND DTOS(CGUD_FEMIS)>=DTOS(DATE()-8)";
                    System.Data.OleDb.OleDbCommand com = new System.Data.OleDb.OleDbCommand(sql_prescripc, dbConn);
                    System.Data.OleDb.OleDbDataAdapter ada = new System.Data.OleDb.OleDbDataAdapter(com);
                    dat_presccabRetail = new DataTable();
                    ada.Fill(dat_presccabRetail);


                    //-- Obtenemos prescripciones RETAIL Detalle
                    string sql_prescripd = "SELECT dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE EMPTY(CGUD_FTXTD) AND DTOS(CGUD_FEMIS)>=DTOS(DATE()-8)";
                    System.Data.OleDb.OleDbCommand comd = new System.Data.OleDb.OleDbCommand(sql_prescripd, dbConn);
                    System.Data.OleDb.OleDbDataAdapter adad = new System.Data.OleDb.OleDbDataAdapter(comd);
                    dat_prescdetRetail = new DataTable();
                    adad.Fill(dat_prescdetRetail);

                }
                catch
                {
                    // omitido
                }


                try
                {

                    //-- Obtenemos prescripciones NO RETAIL Cabecera
                    string sql_prescripc = "SELECT oc_nord,oc_client,oc_secci,oc_almac,oc_fecha,oc_ccli,oc_caden,oc_tipref,oc_docref FROM vmaoc WHERE EMPTY(oc_ftx) and oc_fecha>=date()-7";
                    System.Data.OleDb.OleDbCommand com = new System.Data.OleDb.OleDbCommand(sql_prescripc, dbConn);
                    System.Data.OleDb.OleDbDataAdapter ada = new System.Data.OleDb.OleDbDataAdapter(com);
                    dat_presccabNoRetail = new DataTable();
                    ada.Fill(dat_presccabNoRetail);


                    //-- Obtenemos prescripciones NO RETAIL Detalle
                    string sql_prescripd = "SELECT od_nord,od_cart,od_cali,od_costo,od_qo00,od_qo01,od_qo02,od_qo03,od_qo04,od_qo05,od_qo06,od_qo07,od_qo08,od_qo09,od_qo10,od_qo11 FROM vmaoc INNER JOIN vmaod ON oc_nord=od_nord WHERE EMPTY(oc_ftx) and oc_fecha>=date()-7";
                    System.Data.OleDb.OleDbCommand comd = new System.Data.OleDb.OleDbCommand(sql_prescripd, dbConn);
                    System.Data.OleDb.OleDbDataAdapter adad = new System.Data.OleDb.OleDbDataAdapter(comd);
                    dat_prescdetNoRetail = new DataTable();
                    adad.Fill(dat_prescdetNoRetail);

                }
                catch
                {
                    // omitido
                }

                if ((dat_presccabRetail != null && dat_presccabRetail.Rows.Count > 0) || (dat_presccabNoRetail != null && dat_presccabNoRetail.Rows.Count > 0))
                { exito = true; }

            }

            return exito;
        }


        private bool Obtiene_Prescrip()
        {
            bool exito = false;

            dat_presccabRetail = null;
            dat_prescdetRetail = null;
            dat_presccabNoRetail = null;
            dat_prescdetNoRetail = null;

            string sql = "";

            sql = "SELECT cgud_gudis,cgud_tndcl,cgud_canal,cgud_caden,cgud_almac,cgud_femis FROM SCCCGUD WHERE CGUD_FEMIS>=DATE()-" + wdiasatras.ToString() + " AND EMPTY(CGUD_FTXTD) ORDER BY cgud_gudis ";
            dat_presccabRetail = Conexion.Obt_dbf(sql);

            sql = "SELECT dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE CGUD_FEMIS>=DATE()-" + wdiasatras.ToString() + " AND EMPTY(CGUD_FTXTD) ORDER BY cgud_gudis ";
            dat_prescdetRetail = Conexion.Obt_dbf(sql);

            sql = "SELECT oc_nord,oc_client,oc_canal,oc_secci,oc_almac,oc_fecha,oc_ccli,oc_caden,oc_tipref,oc_docref FROM vmaoc WHERE oc_fecha>=date()-" + wdiasatras.ToString() + " AND EMPTY(oc_ftx) ORDER BY oc_nord ";
            dat_presccabNoRetail = Conexion.Obt_dbf(sql);

            sql = "SELECT od_nord,od_cart,od_cali,od_cpack,od_costo,od_qo00,od_qo01,od_qo02,od_qo03,od_qo04,od_qo05,od_qo06,od_qo07,od_qo08,od_qo09,od_qo10,od_qo11 FROM vmaoc INNER JOIN vmaod ON oc_nord=od_nord WHERE oc_fecha>=date()-" + wdiasatras.ToString() + " AND EMPTY(oc_ftx) ORDER BY oc_nord ";
            dat_prescdetNoRetail = Conexion.Obt_dbf(sql);

            if ((dat_presccabRetail != null && dat_presccabRetail.Rows.Count > 0) || (dat_presccabNoRetail != null && dat_presccabNoRetail.Rows.Count > 0))
            { exito = true; }

            LogHandle.Graba_Log("CONSULTA DATA OK"); // OJO POR MIENTRAS

            return exito;

        }



        private void Archiva_TXT()
        {
            try
            {

                string path = Path.Combine(DatosGenerales.rutaMain, @"BACKUP\");
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                if (File.Exists(fileTXTc))
                {
                    if (File.Exists(path + Path.GetFileName(fileTXTc))) File.Delete(path + Path.GetFileName(fileTXTc));
                    File.Move(fileTXTc, path + Path.GetFileName(fileTXTc)); // Try to move
                }

                if (File.Exists(fileTXTd))
                { 
                    if (File.Exists(path + Path.GetFileName(fileTXTd))) File.Delete(path + Path.GetFileName(fileTXTd));
                    File.Move(fileTXTd, path + Path.GetFileName(fileTXTd)); // Try to move
                }
                                
                
            }
            catch
            {
                // omitido
            }
        }

        private bool Send_FTP_WMS(string file_origen, string file_destino)
        {
            bool exito = false;

            try
            {
                // Setup session options
                SessionOptions sessionOptions = new SessionOptions
                {
                    Protocol = Protocol.Sftp,
                    HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
                    UserName = DatosGenerales.UserFtp, //"retailc"
                    Password = DatosGenerales.PassFtp, //"1wiAwNRa"
                    PortNumber = 22,
                    GiveUpSecurityAndAcceptAnySshHostKey = true
                };

                using (Session session = new Session())
                {

                    // Connect
                    session.Open(sessionOptions);
                    //str.WriteLine("**************** CONECTADO CON EXITO AL FTP " + DateTime.Now);
                    //str.WriteLine("INICIO SUBIDA DE ACHIVO " + NombreArchivo + " AL SFTP " + DateTime.Now);
                    //string nombreAchivoRuta = NombreArchivo + DateTime.Now.ToString("yyyyMMdd") + ".mnt";
                    //string nombreArchivoCompleto = fileTXTc; // "\\\\200.1.1.40\\appl\\pos\\interfaces\\" + nombreAchivoRuta;

                    // Upload files
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.FilePermissions = null; // This is default
                    transferOptions.PreserveTimestamp = false;
                    transferOptions.TransferMode = TransferMode.Binary;
                    TransferOperationResult transferResult;

                    //transferResult = session.PutFiles(fileTXTc, "/data/02_PE/input/" + Path.GetFileName(fileTXTc), false, transferOptions);

                    transferResult = session.PutFiles(file_origen, "/data/02_PE/input/" + Path.GetFileName(file_destino), false, transferOptions);

                    // Throw on any error
                    transferResult.Check();

                    //if (transferResult.IsSuccess == true) exito = true;
                    exito = transferResult.IsSuccess;

                    // Print results
                    //if (exito)
                    //{
                    //    foreach (TransferEventArgs transfer in transferResult.Transfers)
                    //    {
                    //        //varFinal = nombreAchivoRuta + "°" + subido + "°" + "CORRECTAMENTE SUBIDO" + transfer.FileName + " " + DateTime.Now + "°" + "1";
                    //        str.WriteLine("ARCHIVO FUE CARGADO OK: " + transfer.FileName + " " + DateTime.Now);
                    //        //exito = true;
                    //    }
                    //}
                }
            }

            catch (Exception ex)
            {
                //varFinal = string.Empty + "°" + string.Empty + "°" + "[ERROR] NO SE PUDO CARGAR EL DOCUMENTO " + NombreArchivo + " " + DateTime.Now + "°" + "0";
                //str.WriteLine("ERROR AL SUBIR ARCHIVO: " + fileTXTc + " " + e.Message + " " + DateTime.Now);
                LogHandle.Graba_Log("ERROR AL SUBIR FTP: "+ex.Message);
            }


            return exito;
        }


        //private void Generar_Texto(ref String _error)
        //{
        //    using (System.IO.StreamWriter file =
        //    new System.IO.StreamWriter(@"C:\PruebaServicio\WriteLines2.txt", true))
        //    {
        //        file.WriteLine("Fourth line");
        //    }
        //}




    }
}
