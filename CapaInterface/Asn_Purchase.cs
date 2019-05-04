using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WinSCP;
using CapaDatos;
using System.Data;
using System.IO;
using Npgsql;

namespace CapaInterface
{
    public class Resultado_Asn_Purchase
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }

    public class Asn_Purchase
    {

        //************** Envio de Prescripciones       
        string wcodalm = "50001";  //DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        DataSet ds = null;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_Purchase()
        {
            bool exito = false;
            //************** File de texto
            fileTXTc = Path.Combine(DatosGenerales.rutaMain, "ISH_" + fechor);
            fileTXTd = Path.Combine(DatosGenerales.rutaMain, "ISL_" + fechor);

            try
            {

                LogUtil.Graba_Log("ASN", "****** INICIO PROCESO ASN PURCHASE *******");

                if (Obtiene_Asn_Purchase())
                {
                    if (Envia_FTP())
                    {
                        if (Actualiza_Flag_Asn_Purchase())
                        {
                            exito = true;
                        }
                    }

                }
                Archiva_TXT();
                if (exito)
                {
                    LogUtil.Graba_Log("ASN","PROCESO ASN PURCHASE OK");
                }
                else
                {
                    LogUtil.Graba_Log("ASN","PROCESO ASN PURCHASE NADA");
                }


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log("ASN","ERROR: " + ex.ToString());
            }
            finally
            {
                LogUtil.Graba_Log("ASN","******** FIN PROCESO ASN PURCHASE *********");
            }
        }
        //****************************************************************************



        public bool Obtiene_Asn_Purchase()
        {
            bool exito = false;

            DataTable dt_tocompracx = null;
            DataTable dt_tocompradx = null;


            string codcia = "730";
            string sql_tocompracx = "";
            string sql_tocompradx = "";


            //-----------------------
            //------ CABECERA -------
            //-----------------------


            sql_tocompracx = "SELECT C.NRO_PROFORM AS LLAVE01 " +
                            ", C.NRO_OCOMPRA " +
                            ", '50001' AS CD " +
                            ", " + codcia + " AS EMPRESA " +
                            ", '' AS TRAILER_NBR " +
                            ", 'CREATE' AS ACCION " +
                            ", '' AS REF_NBR " + 
                            ", 'NAC' AS TIPO " +
                            ", '' AS LOAD_NBR " +
                            ", '' AS MANIFEST_NBR " +
                            ", '' AS TRAILER_TYPE " +
                            ", '' AS VENDOR_INFO " + 
                            ", '' AS ORIGIN_INFO " +
                            ", '' AS ORIGIN_CODE " +
                            ", '' AS ORIGIN_SHIPPED_UNITS " +
                            ", '' AS LOCK_CODE " +
                            ", TO_CHAR(C.FEC_ENTINI, 'YYYYMMDD') AS FECHA " +
                            ", COD_MONEDA " +
                            "FROM TOCOMPRACX C " +
                            "WHERE C.COD_SECCI NOT IN('M','V') " +
                            "AND C.FLG_TXWMS != 'X' " +
                            "AND C.NRO_OCOMPRA != '' " +
                            "AND C.FEC_EMISION > CURRENT_DATE - 10 AND C.FEC_EMISION <= CURRENT_DATE ";

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Posgre))
            {
                /*selecccionando el archivo TOCOMPRACX */
                using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompracx, cn))
                {
                    cmd.CommandTimeout = 0;
                    using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                    {
                        dt_tocompracx = new DataTable();
                        da.Fill(dt_tocompracx);

                        if ((dt_tocompracx != null && dt_tocompracx.Rows.Count > 0))
                        {
                            exito = true;
                        }
                        else
                        {
                            exito = false;
                            return exito;
                        }
                    }
                    dt_tocompracx.TableName = "TOCOMPRACX";
                }
            }


            ds = new DataSet();
            ds.Tables.Add(dt_tocompracx);

            //// Grabando registro.
            //String[] texto;
            //texto = new string[ds.Tables[0].Rows.Count + 1];

            ////Rellenamos el detalle del fichero
            //String linea;
            //for (int ix = 0; ix < ds.Tables[0].Rows.Count; ix++)
            //{
            //    linea = String.Empty;
            //    for (int j = 0; j < ds.Tables[0].Columns.Count - 1; j++)
            //    {
            //        linea += ds.Tables[0].Rows[ix][j].ToString() + "|";
            //    }
            //    texto[ix] = linea;
            //}

            //// Grabando registro al Archivo de Texto CABECERA
            //File.WriteAllLines(fileTXTc, texto);




            string delimited = "|";
            //bool exito = false;
            var strCab = new StringBuilder();

            for (int ix = 0; ix < ds.Tables[0].Rows.Count; ix++)
            {
                for (int j = 0; j < ds.Tables[0].Columns.Count - 1; j++)
                {
                    strCab.Append(ds.Tables[0].Rows[ix][j].ToString() + delimited);    // Numero de orden de despacho
                    strCab.Append("" + delimited);                                     // RUC destinatario
                    strCab.Append("\r\n");
                }
            }
            if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
            File.WriteAllText(fileTXTc, strCab.ToString());







            //-----------------------
            //----- DETALLE ---------
            //-----------------------
            sql_tocompradx = "SELECT C.NRO_PROFORM AS LLAVE01 " +
                             ", C.NRO_OCOMPRA " +
                             ", '50001' AS CD " +
                             ", " + codcia + " AS EMPRESA " +
                             ", 'CREATE' AS ACCION " +
                             ", 'NAC' AS TIPO " +
                             ", D.COD_PRODUCTO " +
                             ", D.CAN_TOTAL " +
                             ", C.NRO_OCOMPRA " +
                             ", D.COD_SECCI " +
                             ", D.COD_CPACK " +
                             "FROM TOCOMPRACX C " +
                             "LEFT JOIN TOCOMPRAPX D ON C.NRO_OCOMPRA = D.NRO_OCOMPRA " +
                             "WHERE C.COD_SECCI NOT IN('M','V') " +
                             "AND D.FLG_TXWMS != 'X' " +
                             "AND D.FLG_TXWMS != 'X' " +
                             "AND C.NRO_OCOMPRA != '' " +
                             "AND C.FEC_EMISION > CURRENT_DATE - 10 AND C.FEC_EMISION <= CURRENT_DATE ";

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Posgre))
            {
                /*selecccionando el archivo TOCOMPRADX */
                using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompradx, cn))
                {
                    cmd.CommandTimeout = 0;
                    using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                    {
                        dt_tocompradx = new DataTable();
                        da.Fill(dt_tocompradx);

                        if ((dt_tocompradx != null && dt_tocompradx.Rows.Count > 0))
                        {
                            exito = true;
                        }
                        else
                        {
                            exito = false;
                            return exito;
                        }
                    }
                    dt_tocompradx.TableName = "TOCOMPRADX";
                }
            }
            ds.Tables.Add(dt_tocompradx);


            DataTable dtdet = ds.Tables[1];
            //dtdet = Dt_pivot(dtdet);
            StringBuilder str = new StringBuilder();
            int wcorrelativo = 0;
            string worden = "";


            foreach (DataRow fila in dtdet.Rows)
            {
                string valor;
                string cad_envio = "";
                string moneda = "";

                moneda = ds.Tables[0].Rows[0][7].ToString(); // Asignamos Moneda
                valor = fila["cod_cpack"].ToString();


                //if (fila["item"].ToString().Length < 10)
                //{
                //    string val = fila["item"].ToString();
                //}

                if (valor == "00001")// No es Pre-Pack
                {
                    if (fila["can_total"].ToString() != "000")
                    {
                        if (fila["llave01"].ToString() != worden)
                        {
                            wcorrelativo = 0;
                            worden = fila["llave01"].ToString();
                        }

                        wcorrelativo = wcorrelativo + 1;
                        string numero_formateado = wcorrelativo.ToString("000");

                        cad_envio = fila["llave01"].ToString() + "|" + fila["nro_ocompra"].ToString() + "|" + fila["cd"].ToString() + "|" + fila["empresa"].ToString() + "|" + numero_formateado + "|" + fila["accion"].ToString() +
                                            "|" + "|" + "|" + "|" +
                                            fila["cod_producto"].ToString() + fila["cod_secci"].ToString() +
                                            "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["can_total"].ToString() +
                                            "|" + "|" +
                                            fila["nro_ocompra"].ToString();
                    }
                }
                else // Es Pre-Pack
                {
                    if (fila["can_total"].ToString() != "000")
                    {
                        wcorrelativo = wcorrelativo + 1;
                        string numero_formateado = wcorrelativo.ToString("000");

                        cad_envio = fila["llave01"].ToString() + "|" + fila["nro_ocompra"].ToString() + "|" + fila["cd"].ToString() + "|" + fila["empresa"].ToString() + "|" + numero_formateado + "|" + fila["accion"].ToString() +
                                            "|" + "|" + "|" + "|" +
                                            fila["cod_cpack"].ToString() + fila["cod_secci"].ToString() +
                                            "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["can_total"].ToString() +
                                            "|" + "|" +
                                            fila["nro_ocompra"].ToString();
                    }
                }
                str.Append(cad_envio);
                str.Append("\r\n");
            }

            string str2 = str.ToString();

            // Grabando registro al Archivo de Texto DETALLE
            File.WriteAllText(fileTXTd, str2);

            { exito = true; }

            return exito;

        }

        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP()
        {
            bool exito1 = false;
            bool exito2 = false;

            exito1 = Send_FTP_WMS(fileTXTc, fileTXTc);
            exito2 = Send_FTP_WMS(fileTXTd, fileTXTd);

            if (exito1 && exito2) LogUtil.Graba_Log("ASN", "ENVIA FTP ASN PURCHASE OK "); else LogUtil.Graba_Log("ASN", "ENVIA FTP ASN PURCHASE ERROR ");
            return (exito1 && exito2);
        }

        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Asn_Purchase()
        {
            bool exito = false;

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Posgre))
            {
                try
                {
                    if (cn.State == 0) cn.Open();
                    // Grabando al Postgres CABECERA
                    for (int ix = 0; ix < ds.Tables[0].Rows.Count; ix++)
                    {
                        string wnro_ocompra = "";
                        string wsql = "";

                        wnro_ocompra = ds.Tables[1].Rows[ix][1].ToString(); // Elige el campo

                        wsql = "UPDATE TOCOMPRAPX SET FLG_TXWMS = 'X' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
                        using (NpgsqlCommand cmd = new NpgsqlCommand(wsql, cn))
                        {
                            cmd.CommandTimeout = 0;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch
                {
                    if (cn != null)
                        if (cn.State == ConnectionState.Open) cn.Close();
                }
            }

            exito = true;
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


                    // Upload files
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.FilePermissions = null; // This is default
                    transferOptions.PreserveTimestamp = false;
                    transferOptions.TransferMode = TransferMode.Binary;
                    TransferOperationResult transferResult;

                    transferResult = session.PutFiles(file_origen, "/data/730/input/" + Path.GetFileName(file_destino), false, transferOptions);

                    // Throw on any error
                    transferResult.Check();

                    //if (transferResult.IsSuccess == true) exito = true;
                    exito = transferResult.IsSuccess;

                }
            }

            catch (Exception ex)
            {
                LogUtil.Graba_Log("ASN","ERROR AL SUBIR ASN PURCHASE FTP: " + ex.Message);
            }

            return exito;
        }

    }
}