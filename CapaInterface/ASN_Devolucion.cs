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


    public class ASN_Devolucion
    {

        //************** Variables       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string waction = "CREATE";
        string winterface = "DEVOL";
        //int wdiasatras = 7;

        //************** Datatables globales para guardar las devoluciones obtenidas
        DataTable dt_cabe = null;
        DataTable dt_deta = null;
        //DataTable dat_presccabNoRetail = null;
        //DataTable dat_prescdetNoRetail = null;

        //************** Files de texto
        //string nomfiltxt1 = $"ORH{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        //string nomfiltxt2 = $"ORD{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_ASN_Devolucion()
        {
            bool exito = false;
            string wcd = "";

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


                LogUtil.Graba_Log(winterface, "******* INICIO PROCESO *******");

                if (Obtiene_Data())
                {
                    for (int xi = 1; xi <= 2; xi++)
                    {
                        if (xi == 1)
                            wcd = "50001";
                        else
                            wcd = "50003";

                        if (Genera_FileTXT(wcd))
                        {
                            if (Envia_FTP(wcd))
                            {                                
                                if (Actualiza_Flag_Data())
                                {                                    
                                    exito = true;
                                }

                                Archiva_TXT();
                            }
                        }
                    }
                }


                if (exito)
                {
                    LogUtil.Graba_Log(winterface, "SE PROCESO OK"); // OJO POR MIENTRAS
                }
                else
                {
                    LogUtil.Graba_Log(winterface, "NO PROCESO NADA"); // OJO POR MIENTRAS
                }


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, "ERROR: " + ex.ToString());
            }
            finally
            {
                LogUtil.Graba_Log(winterface, "******* FIN PROCESO *******");
            }
        }
        //****************************************************************************



        /************** Actualiza_Flag
        * Metodo que actualiza el flag de envio de las Devoluciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Data()
        {

            bool exito = false;
            string cade = "";
            //var listaCade = new List<string>();

            //DataTable dtaux = null;

            //dtaux = dt_cabe; 


            // OJO FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

            if (dt_cabe != null && dt_cabe.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_cabe.Rows)
                {
                    cade += "'" + Convert.ToString(fila["desc_almac"]).Trim() + Convert.ToString(fila["desc_gudis"]).Trim() + "',";

                    // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
                    //if (cade.Length > 900)
                    //{
                    //    cade = cade.TrimEnd(',');
                    //    listaCade.Add(cade);
                    //    cade = "";
                    //}
                }

                cade = cade.TrimEnd(',');
                //listaCade.Add(cade);


                // RETAIL
                //string conex = "";

                //conex = Conexion.Conn2;                 

                //using (OleDbConnection dbConn = new OleDbConnection(conex))
                //{
                //    dbConn.Open();                    

                //    string sql_upd = "";

                //foreach (var caden in listaCade)

                string sql_upd = "UPDATE BDPOS.dbo.FVDESPC SET FLAG_WMS=1 WHERE ltrim(rtrim(desc_almac)) + ltrim(rtrim(desc_gudis)) IN (" + cade + ")";

                try
                {
                    using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                    {
                        if (cn.State == 0) cn.Open();
                        using (SqlCommand cmd = new SqlCommand(sql_upd, cn))
                        {
                            cmd.CommandText = sql_upd;
                            cmd.CommandTimeout = 0;
                            cmd.ExecuteNonQuery();
                            exito = true;

                            //System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
                            //com_upd.ExecuteNonQuery();
                            //int count = caden.Count(f => f == ',');
                            LogUtil.Graba_Log(winterface, "UPDATE OK Cant Docum: " + Convert.ToString(dt_cabe.Rows.Count));
                        }

                    }

                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(winterface, "ERROR: " + ex.ToString());
                }

            }

            return exito;

        }


        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP(string wcd)
        {
            bool exito1 = false;
            bool exito2 = false;

            exito1 = FTPUtil.Send_FTP_WMS(fileTXTc, fileTXTc, wcd);
            exito2 = FTPUtil.Send_FTP_WMS(fileTXTd, fileTXTd, wcd);

            if (exito1 && exito2)
            { LogUtil.Graba_Log(winterface, "ENVIA FTP OK "); }
            else
            { LogUtil.Graba_Log(winterface, "ENVIA FTP ERROR "); }

            return (exito1 && exito2);
        }


        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT(string wcd)
        {
            bool exito = false;
            string zcd = "";

            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(DatosGenerales.rutaMain, "ISH_" + fechor);
            fileTXTd = Path.Combine(DatosGenerales.rutaMain, "ISL_" + fechor);

            // Eliminar archivos ISH_, ISL
            try
            {
                var dir = new DirectoryInfo(DatosGenerales.rutaMain);
                foreach (var file in dir.EnumerateFiles("IS*.TXT"))
                {
                    file.Delete();
                }
            }
            catch
            {
                // omitido
            }

            if (dt_cabe == null || dt_cabe.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            var str = new StringBuilder();

            foreach (DataRow datarow in dt_cabe.Rows)
            {

                zcd = DatosGenerales.Obt_CDxAlm(datarow["desc_secci"].ToString());
                if (zcd != wcd)
                    continue;

                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(zcd + delimited);                                     // Facility code
                str.Append(wcodcia + delimited);                                 // Cod Cia
                str.Append("" + delimited);
                str.Append(waction + delimited);                                 // Action Code
                str.Append("" + delimited);
                str.Append("DEV" + delimited);                                   // DEV
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(Convert.ToDateTime(datarow["desc_fecha"]).ToString("yyyyMMdd") + delimited);  // Fecha emision
                str.Append("\r\n");
            }

            if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
            File.WriteAllText(fileTXTc, str.ToString());


            // DETALLE
            int correlativo = 0;
            string keyitem = "";
            string grupo = "";

            str = new StringBuilder();

            grupo = dt_deta.Rows[0]["desc_almac"].ToString() + dt_deta.Rows[0]["desc_gudis"].ToString();

            foreach (DataRow datarow in dt_deta.Rows)
            {

                zcd = DatosGenerales.Obt_CDxAlm(datarow["desc_secci"].ToString());
                if (zcd != wcd)
                    continue;

                // Resetear correlativo cuando cambia de grupo
                if (dt_deta.Rows[0]["desc_almac"].ToString() + datarow["desc_gudis"].ToString() != grupo)
                {
                    correlativo = 0;
                    grupo = dt_deta.Rows[0]["desc_almac"].ToString() + dt_deta.Rows[0]["desc_gudis"].ToString();
                }


                //for (int xi = 0; xi < 12; xi++)
                //{

                //string pad = xi.ToString().Trim().PadLeft(2, cero);
                //var value = datarow["desd_med_per"];
                //if (value != DBNull.Value)
                //{
                //int cant = Convert.ToInt32(value);
                //if (cant != 0)
                //{
                correlativo += 1;

                //string pos = (xi + 1).ToString().Trim().PadLeft(2, cero);
                string pos = datarow["desd_med_lat"].ToString();

                keyitem = datarow["desd_artic"].ToString() + datarow["desd_calid"].ToString() + pos + DatosGenerales.CodRetail;

                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(zcd + delimited);                                     // Facility code
                str.Append(wcodcia + delimited);                                 // Cod Cia
                str.Append(correlativo.ToString() + delimited);                  // Numero correlativo
                str.Append(waction + delimited);                                 // Action Code
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(keyitem + delimited);                                 // Key item
                for (int i = 1; i <= 12; i++)
                    str.Append("" + delimited);
                str.Append(correlativo.ToString() + delimited);                  // Cantidad
                str.Append("\r\n");
                //}
                //}
                //}

            }

            if (File.Exists(fileTXTd)) File.Delete(fileTXTd); 
            File.WriteAllText(fileTXTd, str.ToString());

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));
            return exito;

        }




        /************** Obtiene_Prescrip
        * Metodo que obtiene las prescripciones desde el Sis (dbf)
        *****************/
        private bool Obtiene_Data()
        {

            //LogHandle.Graba_Log("PRESC", "ENTRANDO A CONSULTAR DATA"); // OJO POR MIENTRAS

            bool exito = false;

            dt_cabe = null;
            dt_deta = null;
            string msgerror = "";

            // CABECERA
            string sql = "[USP_WMS_Obt_Devoluciones_Tda]";
            //string sql = "select * from BDPOS.dbo.FVDESPC where DESC_FECHA>=GETDATE()-7";
            dt_cabe = Conexion.Obt_SQL(sql, ref msgerror, "C");

            if (msgerror != "")
            {
                LogUtil.Graba_Log(winterface, msgerror);
                return false;
            }

            // DETALLE
            if (dt_cabe != null && dt_cabe.Rows.Count > 0)
            {
                //sql = "SELECT dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE CGUD_FEMIS>=DATE()-" + wdiasatras.ToString() + " AND EMPTY(FLAG_WMS) ORDER BY cgud_gudis ";
                dt_deta = Conexion.Obt_SQL(sql, ref msgerror, "D");

                if (msgerror != "")
                {
                    LogUtil.Graba_Log(winterface, msgerror);
                    return false;
                }
            }

            if (dt_cabe != null && dt_cabe.Rows.Count > 0)
                exito = true;

            LogUtil.Graba_Log(winterface, "CONSULTA DATA OK"); 

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

        //private bool Send_FTP_WMS(string file_origen, string file_destino)
        //{
        //    bool exito = false;

        //    try
        //    {
        //        // Setup session options
        //        SessionOptions sessionOptions = new SessionOptions
        //        {
        //            Protocol = Protocol.Sftp,
        //            HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
        //            UserName = DatosGenerales.UserFtp, //"retailc"
        //            Password = DatosGenerales.PassFtp, //"1wiAwNRa"
        //            PortNumber = 22,
        //            GiveUpSecurityAndAcceptAnySshHostKey = true
        //        };

        //        using (Session session = new Session())
        //        {

        //            // Connect
        //            session.Open(sessionOptions);
        //            //str.WriteLine("**************** CONECTADO CON EXITO AL FTP " + DateTime.Now);
        //            //str.WriteLine("INICIO SUBIDA DE ACHIVO " + NombreArchivo + " AL SFTP " + DateTime.Now);
        //            //string nombreAchivoRuta = NombreArchivo + DateTime.Now.ToString("yyyyMMdd") + ".mnt";
        //            //string nombreArchivoCompleto = fileTXTc; // "\\\\200.1.1.40\\appl\\pos\\interfaces\\" + nombreAchivoRuta;

        //            // Upload files
        //            TransferOptions transferOptions = new TransferOptions();
        //            transferOptions.FilePermissions = null; // This is default
        //            transferOptions.PreserveTimestamp = false;
        //            transferOptions.TransferMode = TransferMode.Binary;
        //            TransferOperationResult transferResult;

        //            transferResult = session.PutFiles(file_origen, "/data/730/input/" + Path.GetFileName(file_destino), false, transferOptions);

        //            // Throw on any error
        //            transferResult.Check();

        //            exito = transferResult.IsSuccess;
   

        //            // Print results
        //            //if (exito)
        //            //{
        //            //    foreach (TransferEventArgs transfer in transferResult.Transfers)
        //            //    {
        //            //        //varFinal = nombreAchivoRuta + "°" + subido + "°" + "CORRECTAMENTE SUBIDO" + transfer.FileName + " " + DateTime.Now + "°" + "1";
        //            //        str.WriteLine("ARCHIVO FUE CARGADO OK: " + transfer.FileName + " " + DateTime.Now);
        //            //        //exito = true;
        //            //    }
        //            //}
        //        }
        //    }

        //    catch (Exception ex)
        //    {
        //        //varFinal = string.Empty + "°" + string.Empty + "°" + "[ERROR] NO SE PUDO CARGAR EL DOCUMENTO " + NombreArchivo + " " + DateTime.Now + "°" + "0";
        //        //str.WriteLine("ERROR AL SUBIR ARCHIVO: " + fileTXTc + " " + e.Message + " " + DateTime.Now);
        //        LogHandle.Graba_Log(winterface, "ERROR AL SUBIR FTP: " + ex.Message);
        //    }


        //    return exito;
        //}


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

