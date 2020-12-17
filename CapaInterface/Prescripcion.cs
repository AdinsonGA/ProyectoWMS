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
using System.Text.RegularExpressions;


//using System.Data.SqlTypes;

namespace CapaInterface
{

    //public class Resultado
    //{
    //    public bool Exito { get; set; }
    //    public string MensajeError { get; set; }
    //    public DataSet DatosDevueltos { get; set; }
    //}
    public class Prescripcion
    {

        //************** Variables       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string waction = "CREATE";
        string winterface = "PRESC";
        string wtablafuente = "";
        string wcade_update = "";
        List<string> listaCade = new List<string>();

        //int wdiasatras = 57;

        //************** Datatables globales para guardar las prescripciones obtenidas
        DataTable dt_cab = null;
        DataTable dt_det = null;
        //DataTable dt_cab_noretail = null;
        //DataTable dt_det_noretail = null;

        //************** Files de texto
        //string nomfiltxt1 = $"ORH{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        //string nomfiltxt2 = $"ORD{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_Prescripcion(int file1_file2)
        {
            bool exito = false;
            string wcd = "";

            wtablafuente = (file1_file2 == 1) ? "SCCCGUD" : "VMAOC";

            try
            {
                // Verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface("WMS");

                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M001"] + wtablafuente, false, ""); //MSJ INICIO DE PROCESO

                if (Obtiene_Prescrip(file1_file2))
                {
                    for (int xi = 1; xi <= 2; xi++)
                    {
                        wcd = (xi == 1) ? "50001" : "50003";

                        if (Genera_FileTXT(file1_file2, wcd))
                        {
                            if (Envia_FTP(wcd))
                            {
                                if (Actualiza_Flag_Prescrip(file1_file2))
                                {
                                    exito = true;
                                }

                                Archiva_TXT(wcd);
                            }
                        }
                    }

                }

                //if (exito)
                //{
                //    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M002"], false, ""); // MSJ SE PROCESO LA DATA OK
                //}
                //else
                //{
                //    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M003"], false, ""); // MSJ NO HUBO PROCESO
                //}

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            }
            finally
            {
                //var resul = (exito) ? "OK" : "CON ERRORES";
                LogUtil.Graba_Log(winterface, winterface + " ***** Fin de Proceso ***** " + ((exito) ? "OK" : " "), false, ""); // MSJ FIN DE PROCESO DE DATA
            }
        }
        //****************************************************************************


        /************** Actualiza_Flag_Prescrip
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        //private bool Actualiza_Flag_Prescrip(int file1_file2, string wcd)
        //{

        //    bool exito = false;

        //    try
        //    {
        //        exito = Actualiza_Flag(file1_file2, wcd);                
        //    }
        //    catch (Exception ex)
        //    {
        //        LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
        //    }

        //    return (exito);

        //}

        private bool Actualiza_Flag_Prescrip(int file1_file2)
        {
            bool exito = false;

            //string cade = "";
            //var listaCade = new List<string>();
            //string zcd = "";
            //string descrip = (file1_file2 == 1) ? "SCCCGUD" : "VMAOC";
            //string campo = (file1_file2 == 1) ? "cgud_almac" : "oc_almac";

            // OJO FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

            //if (dt_cab != null && dt_cab.Rows.Count > 0)
            //{
            //    foreach (DataRow fila in dt_cab.Rows)
            //    {

            //        zcd = DatosGenerales.Obt_CDxAlm(fila[campo].ToString());
            //        if (zcd == wcd)
            //        {

            //            //if (file1_file2 == 1)
            //            //{
            //            cade += "'" + Convert.ToString(fila[campo]).Trim() + "',";
            //            //}
            //            //else
            //            //{
            //            //    cade += "'" + Convert.ToString(fila["oc_nord"]).Trim() + "',";
            //            //}

            //            // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
            //            if (cade.Length > 900)
            //            {
            //                cade = cade.TrimEnd(',');
            //                listaCade.Add(cade);
            //                cade = "";
            //            }
            //        }
            //    }

            //    cade = cade.TrimEnd(',');
            //    listaCade.Add(cade);

            if (listaCade.Count() == 0)
            {
                return true;
            }

            string conex = "";

            // HACER EL UPDATE EN LA TABLA RESPECTIVA
            conex = (file1_file2 == 1) ? Conexion.Conn2 : Conexion.Conn1;

            using (OleDbConnection dbConn = new OleDbConnection(conex))
            {
                dbConn.Open();

                string sql_upd = "";

                foreach (var caden in listaCade)
                {
                    if (file1_file2 == 1)
                    {
                        sql_upd = "UPDATE SCCCGUD SET FLAG_WMS='X' WHERE cgud_gudis IN (" + caden + ")";
                    }
                    else
                    {
                        sql_upd = "UPDATE vmaoc SET FLAG_WMS='X' WHERE oc_nord IN (" + caden + ")";
                    }
                    System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
                    com_upd.CommandTimeout = 0;
                    com_upd.ExecuteNonQuery();
                    int count = caden.Count(f => f == ',');
                    LogUtil.Graba_Log(winterface, winterface + " : Se actualizó en " + wtablafuente + " Documentos: " + Convert.ToString(count + 1), false, "");
                }

            }


            //}

            //// NO RETAIL

            //if (dt_cab_noretail != null && dt_cab_noretail.Rows.Count > 0)
            //{
            //    cade = "";

            //    foreach (DataRow fila in dt_cab_noretail.Rows)
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
        private bool Envia_FTP(string wcd)
        {
            bool exito1 = false;
            bool exito2 = false;

            exito1 = FTPUtil.Send_FTP_WMS(fileTXTc, fileTXTc, wcd, winterface);
            exito2 = FTPUtil.Send_FTP_WMS(fileTXTd, fileTXTd, wcd, winterface);

            if (exito1 && exito2)
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M006"].ToString(), false, "");  // MSJ SE ENVIO AL FTP OK
            else
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M007"].ToString(), true, "");  // MSJ NO SE ENVIO AL FTP

            return (exito1 && exito2);
        }


        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT(int file1_file2, string wcd)
        {

            bool exito = false;

            wcade_update = "";
            listaCade.Clear();

            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(Crear_Carpetas.WORK, "ORH_PRES_" + fechor);
            fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ORD_PRES_" + fechor);

            // Eliminar archivos ORH, ORD.TXT
            try
            {
                if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
                if (File.Exists(fileTXTd)) File.Delete(fileTXTd);

                //var dir = new DirectoryInfo(Crear_Carpetas.WORK);
                //foreach (var file in dir.EnumerateFiles("ORH_*.TXT"))
                //{
                //    file.Delete();
                //}
                //foreach (var file in dir.EnumerateFiles("ORD_*.TXT"))
                //{
                //    file.Delete();
                //}

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR AL ELIMINAR FILES: " + ex.ToString(), true, "");
                throw ex;
            }

            //if (File.Exists(fileTXTc)) { File.Delete(fileTXTc); }
            //if (File.Exists(fileTXTd)) { File.Delete(fileTXTd); }

            if (file1_file2 == 1)
                exito = Genera_FileTXT_1(wcd);
            else
                exito = Genera_FileTXT_2(wcd);

            return (exito);
        }


        private bool Genera_FileTXT_1(string wcd)
        {

            if (dt_cab == null || dt_cab.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            bool exito = false;
            string zcd = "";

            var str = new StringBuilder();

            foreach (DataRow datarow in dt_cab.Rows)
            {
                zcd = DatosGenerales.Obt_CDxAlm(datarow["cgud_almac"].ToString());
                if (zcd == wcd)
                {
                    str.Append(datarow["cgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                    str.Append(zcd + delimited);                                     // Facility code
                    str.Append(wcodcia + delimited);                                 // Cod Cia
                    str.Append(datarow["cgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                    str.Append(datarow["cgud_canal"].ToString() + datarow["cgud_almac"].ToString() + delimited);  // Order Type ejemplo: 5K
                    str.Append(Convert.ToDateTime(datarow["cgud_femis"]).ToString("yyyyMMdd") + delimited);        // Fecha emision
                    str.Append("" + delimited);                                      // exp_date
                    str.Append(Convert.ToDateTime(datarow["cgud_femis"]).ToString("yyyyMMdd") + delimited);        // Fecha de entrega requerida

                    // 16-12-2020 ALMACEN I SE ENVIA CON CLIENTE GENERICO, SOLICITUD DE OFRACIO
                    var wcliente = (datarow["cgud_almac"].ToString() == "I") ? "92070" : datarow["cgud_tndcl"].ToString();

                    str.Append(wcliente + delimited);        // dest_facility_code (cod tienda)   
                    //str.Append(datarow["cgud_tndcl"].ToString() + delimited);        // dest_facility_code (cod tienda)                    

                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    //str.Append("" + delimited);
                    str.Append(datarow["cgud_tndcl"].ToString() + delimited);        // ref_nbr (cod tienda/cliente original) 

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

                    str.Append(wcliente + delimited);       // Tienda - COD CLIENTE
                    //str.Append(datarow["cgud_tndcl"].ToString() + delimited);       // Tienda - COD CLIENTE

                    for (int i = 1; i <= 17; i++)
                    { str.Append("" + delimited); };

                    str.Append(datarow["cgud_gudis"].ToString() + delimited);      // Customer_PO_number (nro prescrip - Juan V.)
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


                    // ACUMULAMOS EN UNA LISTA PARA HACER EL UPDATE DEL FLAG MAS ABAJO
                    wcade_update += "'" + Convert.ToString(datarow["cgud_gudis"]).Trim() + "',";

                    // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
                    if (wcade_update.Length > 900)
                    {
                        wcade_update = wcade_update.TrimEnd(',');
                        listaCade.Add(wcade_update);
                        wcade_update = "";
                    }
                }
            }

            if (str.Length == 0) return false;

            File.WriteAllText(fileTXTc, str.ToString());

            wcade_update = wcade_update.TrimEnd(',');
            listaCade.Add(wcade_update);


            // DETALLE RETAIL
            int correlativo = 0;
            string keyitem = null;
            char cero = '0';
            string grupo = "";

            str = new StringBuilder();

            grupo = dt_det.Rows[0]["dgud_gudis"].ToString();

            foreach (DataRow datarow in dt_det.Rows)
            {
                zcd = DatosGenerales.Obt_CDxAlm(datarow["cgud_almac"].ToString());
                if (zcd == wcd)
                {

                    // Resetear correlativo cuando cambia de grupo
                    if (datarow["dgud_gudis"].ToString() != grupo)
                    {
                        correlativo = 0;
                        grupo = datarow["dgud_gudis"].ToString();
                    }


                    for (int xi = 0; xi < 12; xi++)
                    {

                        string pad = xi.ToString().Trim().PadLeft(2, cero);
                        var value = datarow["dgud_med" + pad];
                        if (value != DBNull.Value)
                        {
                            int cant = Convert.ToInt32(value);
                            if (cant != 0)
                            {
                                correlativo += 1;
                                decimal costo = 0;

                                // Evaluar si el articulo es prepack o suelto
                                if (datarow["dgud_cpack"].ToString() == "00001")
                                {
                                    int posi = xi;

                                    // Ajustar medida (Conversado con Vicente)
                                    //if (datarow["dgud_rmed"].ToString() == "E")
                                    //    posi = 0;
                                    //else
                                    //if ((datarow["dgud_artic"].ToString().Substring(0, 1) == "9") && (new[] { "A", "B", "C", "D", "E" }.Contains(datarow["dgud_rmed"].ToString())))
                                    //    posi = 0;

                                    // Para el WMS se suma 1 a la posicion
                                    string pos = (posi + 1).ToString().Trim().PadLeft(2, cero);

                                    //keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + pos + DatosGenerales.CodRetail;
                                    keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + pos;
                                    costo = Convert.ToDecimal(datarow["dgud_costo"]);
                                }
                                else
                                {
                                    //keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + datarow["dgud_cpack"].ToString() + DatosGenerales.CodRetail;
                                    keyitem = datarow["dgud_artic"].ToString() + datarow["dgud_calid"].ToString() + datarow["dgud_cpack"].ToString();
                                    cant = Convert.ToInt32(datarow["dgud_ppack"]);
                                    costo = Convert.ToDecimal(datarow["dgud_costo"]) * Convert.ToInt32(datarow["dgud_touni"]);
                                }

                                str.Append(datarow["dgud_gudis"].ToString() + delimited);        // Numero de orden de despacho
                                str.Append(DatosGenerales.Obt_CDxAlm(datarow["cgud_almac"].ToString()) + delimited);  // Facility code
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
                                str.Append(costo.ToString() + delimited);                        // Costo
                                str.Append("0" + delimited);                                     // Sales
                                for (int i = 1; i <= 16; i++)
                                { str.Append("" + delimited); };
                                //str.Append(DateTime.Now.ToString("yyyyMMdd") + delimited);       // voucher_exp_date
                                str.Append("" + delimited);                                        // voucher_exp_date (AHORA VA EN BLANCO)
                                str.Append("\r\n");

                                // Si es prepack se genera solo 1 fila en el TXT (correo de Miguel O.)
                                if (datarow["dgud_cpack"].ToString() != "00001")
                                    break;
                            }
                        }
                    }
                }

            }

            if (str.Length == 0) return false;

            File.WriteAllText(fileTXTd, str.ToString());

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));

            if (exito)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M012"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); // MSJ SE GENERO LOS ARCHIVOS OK
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M013"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); // MSJ ERROR AL GENERAR ARCHIVOS
            }

            return exito;
        }

        private bool Genera_FileTXT_2(string wcd)
        {

            if (dt_cab == null || dt_cab.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            bool exito = false;
            string zcd = "";
            var str = new StringBuilder();

            foreach (DataRow datarow in dt_cab.Rows)
            {
                zcd = DatosGenerales.Obt_CDxAlm(datarow["oc_almac"].ToString());
                if (zcd == wcd)
                {

                    str.Append(datarow["oc_nord"].ToString() + delimited);           // Numero de orden de despacho
                    str.Append(zcd + delimited);                                     // Facility code
                    str.Append(wcodcia + delimited);                                 // Cod Cia
                    str.Append(datarow["oc_nord"].ToString() + delimited);           // Numero de orden de despacho
                    str.Append(datarow["oc_canal"].ToString() + datarow["oc_almac"].ToString() + delimited);  // Order Type ejemplo: 5K
                    str.Append(Convert.ToDateTime(datarow["oc_fecha"]).ToString("yyyyMMdd") + delimited);     // Fecha emision
                    str.Append("" + delimited);                                      // exp_date
                    str.Append(Convert.ToDateTime(datarow["oc_fecha"]).ToString("yyyyMMdd") + delimited);     // Fecha de entrega requerida

                    var xtda = datarow["oc_client"].ToString().Trim();

                    if (datarow["oc_almac"].ToString() == "4" || datarow["oc_almac"].ToString() == "6")
                    {
                        if (!(datarow["oc_clisuc"].ToString().Trim() == null || datarow["oc_clisuc"].ToString().Trim() == ""))
                            xtda = xtda + datarow["oc_clisuc"].ToString().Trim();
                    }
                    else
                    {
                        // 16-12-2020 ALMACEN I SE ENVIA CON CLIENTE GENERICO, SOLICITUD DE OFRACIO
                        if (datarow["oc_almac"].ToString() == "I")
                            xtda = "92070";
                    }

                    str.Append(xtda + delimited);                                    // dest_facility_code (cod cliente)
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
                    str.Append(xtda + delimited);                                    // shipto_facility_code (Tienda)

                    for (int i = 1; i <= 11; i++)
                    { str.Append("" + delimited); };

                    if (datarow["oc_almac"].ToString() == "4" || datarow["oc_almac"].ToString() == "6")
                        str.Append(datarow["oc_client"].ToString() + delimited);     // dest_company_code  (Cliente)
                    else
                        str.Append("" + delimited);

                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    str.Append("" + delimited);
                    str.Append("" + delimited);

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

                    for (int i = 1; i <= 25; i++)
                    { str.Append("" + delimited); };

                    str.Append(datarow["oc_client"].ToString().Trim() + delimited);  // hdr_cust_short_text_5 (cliente original)

                    str.Append("\r\n");

                    // ACUMULAMOS EN UNA LISTA PARA HACER EL UPDATE DEL FLAG MAS ABAJO
                    wcade_update += "'" + Convert.ToString(datarow["oc_nord"]).Trim() + "',";

                    // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
                    if (wcade_update.Length > 900)
                    {
                        wcade_update = wcade_update.TrimEnd(',');
                        listaCade.Add(wcade_update);
                        wcade_update = "";
                    }
                }
            }

            if (str.Length == 0) return false;

            File.WriteAllText(fileTXTc, str.ToString());

            wcade_update = wcade_update.TrimEnd(',');
            listaCade.Add(wcade_update);


            // DETALLE NORETAIL
            int correlativo = 0;
            string keyitem = null;
            char cero = '0';
            string grupo = "";

            str = new StringBuilder();

            grupo = dt_det.Rows[0]["od_nord"].ToString();

            foreach (DataRow datarow in dt_det.Rows)
            {
                zcd = DatosGenerales.Obt_CDxAlm(datarow["oc_almac"].ToString());
                if (zcd == wcd)
                {

                    // Resetear correlativo cuando cambia de grupo
                    if (datarow["od_nord"].ToString() != grupo)
                    {
                        correlativo = 0;
                        grupo = datarow["od_nord"].ToString();
                    }

                    for (int xi = 0; xi < 12; xi++)
                    {

                        string pad = xi.ToString().Trim().PadLeft(2, cero);
                        var value = datarow["od_qo" + pad];
                        if (value != DBNull.Value)
                        {
                            int cant = Convert.ToInt32(value);
                            if (cant != 0)
                            {
                                correlativo += 1;

                                // Evaluar si el articulo es prepack o suelto
                                if (datarow["od_cpack"].ToString() == "00001" || datarow["od_cpack"].ToString().Trim() == String.Empty)
                                {
                                    int posi = xi;

                                    //// Ajustar medida (Conversado con Vicente)
                                    //if (datarow["od_rmed"].ToString() == "E")
                                    //    posi = 0;
                                    //else
                                    //if ((datarow["od_cart"].ToString().Substring(0, 1) == "9") && (new[] { "A", "B", "C", "D", "E" }.Contains(datarow["od_rmed"].ToString())))
                                    //    posi = 0;

                                    // Para el WMS se suma 1 a la posicion
                                    string pos = (posi + 1).ToString().Trim().PadLeft(2, cero);

                                    //keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + pos + DatosGenerales.CodNoRetail;
                                    keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + pos;
                                }
                                else
                                {
                                    //keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + datarow["od_cpack"].ToString() + DatosGenerales.CodNoRetail;
                                    keyitem = datarow["od_cart"].ToString() + datarow["od_cali"].ToString() + datarow["od_cpack"].ToString();
                                }

                                str.Append(datarow["od_nord"].ToString() + delimited);            // Numero de orden de despacho
                                str.Append(zcd + delimited);                                      // Facility code
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
                                str.Append("0" + delimited);                                      // Sales
                                for (int i = 1; i <= 16; i++)
                                { str.Append("" + delimited); };
                                //str.Append(DateTime.Now.ToString("yyyyMMdd") + delimited);       // voucher_exp_date
                                str.Append("" + delimited);                                        // voucher_exp_date (AHORA VA EN BLANCO)
                                str.Append("\r\n");

                                // Si es prepack se genera solo 1 fila en el TXT (correo de Miguel O.)
                                if (!(datarow["od_cpack"].ToString() == "00001" || datarow["od_cpack"].ToString().Trim() == String.Empty))
                                    break;
                            }
                        }
                    }
                }
            }

            if (str.Length == 0) return false;

            File.WriteAllText(fileTXTd, str.ToString());

            //using (StreamWriter filtxt = new StreamWriter(fileTXTd, true, System.Text.Encoding.Default))
            //{
            //    filtxt.WriteLine(str.ToString());
            //}

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));

            if (exito)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M012"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); // MSJ SE GENERO LOS ARCHIVOS OK
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M013"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); // MSJ ERROR AL GENERAR ARCHIVOS
            }

            return exito;
        }



        /************** Obtiene_Prescrip
        * Metodo que obtiene las prescripciones desde el Sis (dbf)
        *****************/
        private bool Obtiene_Prescrip(int file1_file2)
        {

            //LogHandle.Graba_Log(winterface, "ENTRANDO A CONSULTAR DATA"); // OJO POR MIENTRAS

            string Dias = ConfigurationManager.AppSettings["dias"].ToString();

            bool exito = false;

            dt_cab = null;
            dt_det = null;
            //dt_cab_noretail = null;
            //dt_det_noretail = null;

            string sql = "";

            if (file1_file2 == 1)
            {
                // CABECERA RETAIL
                sql = "SELECT cgud_gudis,cgud_tndcl,cgud_canal,cgud_caden,cgud_almac,cgud_femis FROM SCCCGUD WHERE CGUD_FEMIS>=DATE()-" + Dias + " AND EMPTY(FLAG_WMS) AND CGUD_EMPRE!='06' AND (CGUD_ESTAD='Pre' OR CGUD_ESTAD='Pro') ORDER BY cgud_gudis ";
                dt_cab = Conexion.Obt_dbf(sql, file1_file2);

                // DETALLE RETAIL
                if (dt_cab != null && dt_cab.Rows.Count > 0)
                {
                    sql = "SELECT cgud_almac,dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_ppack,dgud_rmed,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE CGUD_FEMIS>=DATE()-" + Dias + " AND EMPTY(FLAG_WMS) AND CGUD_EMPRE!='06' AND (CGUD_ESTAD='Pre' OR CGUD_ESTAD='Pro') ORDER BY cgud_gudis ";
                    dt_det = Conexion.Obt_dbf(sql, file1_file2);
                }

            }

            if (file1_file2 == 2)
            {
                // CABECERA NO RETAIL
                sql = "SELECT oc_nord,oc_client,oc_canal,oc_secci,oc_almac,oc_fecha,oc_ccli,oc_clisuc,oc_caden,oc_tipref,oc_docref FROM vmaoc WHERE oc_fecha>=date()-" + Dias + " AND EMPTY(FLAG_WMS) AND OC_EMPRE!='06' AND (OC_ESTADO='Pre' OR OC_ESTADO='Pro') ORDER BY oc_nord ";
                dt_cab = Conexion.Obt_dbf(sql, file1_file2);

                // DETALLE NO RETAIL
                if (dt_cab != null && dt_cab.Rows.Count > 0)
                {
                    sql = "SELECT oc_almac,od_nord,od_cart,od_cali,od_cpack,od_costo,od_rmed,od_qo00,od_qo01,od_qo02,od_qo03,od_qo04,od_qo05,od_qo06,od_qo07,od_qo08,od_qo09,od_qo10,od_qo11 FROM vmaoc INNER JOIN vmaod ON oc_nord=od_nord WHERE oc_fecha>=date()-" + Dias + " AND EMPTY(FLAG_WMS) AND OC_EMPRE!='06' AND (OC_ESTADO='Pre' OR OC_ESTADO='Pro') ORDER BY oc_nord ";
                    dt_det = Conexion.Obt_dbf(sql, file1_file2);
                }
            }

            if (dt_cab != null && dt_cab.Rows.Count > 0) //|| (dt_cab_noretail != null && dt_cab_noretail.Rows.Count > 0))
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M008"].ToString(), false, ""); // MSJ CONSULTA OK
                exito = true;
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M010"].ToString(), false, ""); // MSJ NO HAY DATOS PARA PROCESAR
                //REVISAR CON ADINSON exito = true;
            }

            return exito;
        }


        private void Archiva_TXT(string CodAlmacen)
        {
            try
            {
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(CodAlmacen);


                if (File.Exists(fileTXTc)) //cabecera
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc));
                        File.Move(fileTXTc, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc));
                        File.Move(fileTXTc, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc)); // Try to move
                    }
                }

                if (File.Exists(fileTXTd)) //detalle
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd));
                        File.Move(fileTXTd, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd));
                        File.Move(fileTXTd, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd)); // Try to move
                    }
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            }
        }

    }
}
