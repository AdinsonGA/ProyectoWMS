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
        string winterface = "ASN_DEVOL";
        //int wdiasatras = 7;
        string wcd;
        //string wcd2 = "50003";
        string alm_lurin = "BA2";

        //************** Datatables globales para guardar las devoluciones obtenidas
        DataTable dt_cab = null;
        DataTable dt_det = null;
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
            //string wcd = "";

            try
            {
                //verifica si existe la carpeta work antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface("WMS");

                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M001"].ToString(), false, ""); //  MSJ INICIO DE PROCESO

                if (Obtiene_Data())
                {
                    //for (int xi = 1; xi <= 2; xi++)
                    //{
                    //    if (xi == 1)
                    //        wcd = "50001";
                    //    else
                    //        wcd = "50003";

                    for (int ii = 1; ii <= 1; ii++)
                    {
                        if (ii == 1)
                        {
                            wcd = "50001";
                        }
                        else
                        {
                            wcd = "50003";
                        }

                        if (Genera_FileTXT(wcd))
                        {
                            if (Envia_FTP(wcd))
                            {
                                Archiva_TXT(wcd);

                            }
                        }
                    }

                    if (Actualiza_Flag_Data())
                    {
                        exito = true;
                    }

                    //}
                }

                if (exito)
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M002"].ToString(), false, ""); // MSJ SE PROCESO LA DATA OK
                }
                else
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M003"].ToString(), false, ""); // MSJ NO HAY DATOS PARA PROCESAR
                }
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, "ERROR: " + ex.ToString(), true, "");
            }
            finally
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M004"].ToString(), false, ""); // MSJ FIN DE PROCESO DE DATA 
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

            //dtaux = dt_cab; 


            // OJO FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_cab.Rows)
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

                            if (cn != null)
                                if (cn.State == ConnectionState.Open) cn.Close();

                            //System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
                            //com_upd.ExecuteNonQuery();
                            //int count = caden.Count(f => f == ',');
                            LogUtil.Graba_Log(winterface, winterface + " : Se actualizó : " + Convert.ToString(dt_cab.Rows.Count) + " Documentos", false, "");
                        }

                    }

                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(winterface, "ERROR: " + ex.ToString(), true, "");
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

            //return true; // ojo x mientras COMENTADO

            exito1 = FTPUtil.Send_FTP_WMS(fileTXTc, fileTXTc, wcd, winterface);
            exito2 = FTPUtil.Send_FTP_WMS(fileTXTd, fileTXTd, wcd, winterface);

            if (exito1 && exito2)
            { LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M006"].ToString(), false, ""); } // MSJ SE ENVIO AL FTP OK
            else
            { LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M007"].ToString(), true, ""); } // NO SE ENVIO AL FTP

            return (exito1 && exito2);
        }


        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT(string wcd)
        {
            bool exito = false;

            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            if (wcd == "50001")
            {
                fileTXTc = Path.Combine(Crear_Carpetas.WORK, "ISH_DEV_TDA_CHO_" + fechor);
                fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ISL_DEV_TDA_CHO_" + fechor);
            }
            else
            {
                fileTXTc = Path.Combine(Crear_Carpetas.WORK, "ISH_DEV_TDA_LUR_" + fechor);
                fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ISL_DEV_TDA_LUR_" + fechor);
            }


            // Eliminar archivos ISH_DEV_TDA, ISL_DEV_TDA.TXT
            try
            {
                if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
                if (File.Exists(fileTXTd)) File.Delete(fileTXTd);
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR AL ELIMINAR FILES: " + ex.ToString(), true, "");
                throw ex;
            }


            if (dt_cab == null || dt_cab.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            var str = new StringBuilder();

            foreach (DataRow datarow in dt_cab.Rows)
            {
                //zcd = DatosGenerales.Obt_CDxAlm(datarow["desc_secci"].ToString());

                //if (zcd != wcd)
                //    continue;

                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(wcd + delimited);                                     // Facility code
                str.Append(wcodcia + delimited);                                 // Cod Cia
                str.Append("" + delimited);
                str.Append(waction + delimited);                                 // Action Code
                str.Append("" + delimited);
                str.Append("DEV" + delimited);                                   // DEV
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["cod_asociado"].ToString() + delimited); // codigo de asociado **
                str.Append(datarow["desc_almac"].ToString() + delimited);        // Tienda
                str.Append("" + delimited);
                str.Append("" + delimited);

                if (wcd == "50001")
                {
                    str.Append(datarow["lockcode"].ToString() + delimited);         // Lock Code (cquinto nuevo)
                }
                else
                {
                    str.Append(alm_lurin + delimited);
                }

                str.Append(Convert.ToDateTime(datarow["desc_fecha"]).ToString("yyyyMMdd") + delimited);  // Fecha emision
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["ruc"] + delimited); // ruc del asociado
                str.Append(datarow["desc_num_debito"] + delimited); //Numero de nota de debito
                str.Append("\r\n");
            }

            //if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
            if (str.Length == 0) return false;

            File.WriteAllText(fileTXTc, str.ToString());


            // DETALLE
            int correlativo = 0;
            string keyitem = "";
            string grupo = "";
            string cadalm;

            str = new StringBuilder();

            grupo = dt_det.Rows[0]["desc_almac"].ToString() + dt_det.Rows[0]["desc_gudis"].ToString();

            foreach (DataRow datarow in dt_det.Rows)
            {

                //zcd = DatosGenerales.Obt_CDxAlm(datarow["desc_secci"].ToString());
                //if (zcd != wcd)
                //    continue;

                // Resetear correlativo cuando cambia de grupo
                if (dt_det.Rows[0]["desc_almac"].ToString() + datarow["desc_gudis"].ToString() != grupo)
                {
                    correlativo = 0;
                    grupo = dt_det.Rows[0]["desc_almac"].ToString() + dt_det.Rows[0]["desc_gudis"].ToString();
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
                string pos = datarow["desd_med_aju"].ToString();
                string cod_almac = datarow["desd_almac_aju"].ToString(); //Campo coregido cod_almacen sacado del postgress

                keyitem = datarow["desd_artic"].ToString() + datarow["desd_calid"].ToString() + pos; //+ DatosGenerales.CodRetail;
                //cadalm = datarow["desc_caden"].ToString() + datarow["desc_almac"].ToString();
                cadalm = datarow["desc_caden"].ToString() + cod_almac;

                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(datarow["desc_ndesp"].ToString() + delimited);        // Numero de guia
                str.Append(wcd + delimited);                                     // Facility code

                str.Append(wcodcia + delimited);                                 // Cod Cia
                str.Append(correlativo.ToString() + delimited);                  // Numero correlativo
                str.Append(waction + delimited);                                 // Action Code
                str.Append(datarow["desc_ndesp"].ToString() + delimited);        //Numero de guia (*)
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(keyitem + delimited);                                 // Key item
                for (int i = 1; i <= 9; i++)
                { str.Append("" + delimited); }
                str.Append("100" + delimited);                                   // valor fijo (*)
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(datarow["desd_pares"]
                    .ToString() + delimited);        // cantidad a devolver
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                //str.Append(cadalm + delimited);        // cadena + almacen (*)

                if (wcd == "50001")
                {
                    str.Append(datarow["TIPO_CADENA"] + delimited); // nuevo campo PUTAWAY_TYPE 
                }
                else
                {
                    str.Append(alm_lurin + delimited); // nuevo campo PUTAWAY_TYPE 
                }
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append("" + delimited);
                str.Append(keyitem + delimited);                                 // Key item (*)
                str.Append(datarow["desd_prvta"].ToString() + delimited);        // precio articulo (*)
                str.Append(datarow["COD_PROVE"].ToString() + delimited);        // codigo de proveedor(*)
                str.Append("\r\n");
                //}
                //}
                //}

            }

            //if (File.Exists(fileTXTd)) File.Delete(fileTXTd);
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


        /************** Obtiene_Prescrip
        * Metodo que obtiene las prescripciones desde el Sis (dbf)
        *****************/
        private bool Obtiene_Data()
        {
            Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

            bool exito = false;

            dt_cab = null;
            dt_det = null;
            string msgerror = "";

            // CABECERA
            string sql = "[USP_WMS_Obt_Devoluciones_Tda_2]";
            //string sql = "select * from BDPOS.dbo.FVDESPC where DESC_FECHA>=GETDATE()-7";
            dt_cab = Conexion.Obt_SQL(sql, ref msgerror, "C", Dias);


            if (msgerror != "")
            {
                LogUtil.Graba_Log(winterface, msgerror, true, "");
                return false;
            }

            // DETALLE
            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {
                //sql = "SELECT dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE CGUD_FEMIS>=DATE()-" + wdiasatras.ToString() + " AND EMPTY(FLAG_WMS) ORDER BY cgud_gudis ";
                dt_det = Conexion.Obt_SQL(sql, ref msgerror, "D", Dias);

                if (msgerror != "")
                {
                    LogUtil.Graba_Log(winterface, msgerror, true, "");
                    return false;
                }
            }

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M008"].ToString(), false, ""); // CONSULTA DE DATOS OK
                exito = true;
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M010"].ToString(), false, ""); // NO HAY DATOS PARA PROCESAR
            }

            return exito;
        }

        private void Archiva_TXT(string wcd)
        {
            try
            {
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(wcd);

                if (File.Exists(fileTXTc))
                {
                    if (wcd == "50001") //cabecera
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

                if (File.Exists(fileTXTd))//detalle
                {
                    if (wcd == "50001")
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
                LogUtil.Graba_Log(winterface, "ERROR: " + ex.ToString(), true, "");
            }
        }

    }
}

