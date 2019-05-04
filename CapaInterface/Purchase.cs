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
    public class Resultado_Purchase
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }

    public class Purchase
    {

        //************** Envio de Prescripciones       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        DataSet ds = null;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_Purchase()
        {
            bool exito = false;
            //************** File de texto
            fileTXTc = Path.Combine(DatosGenerales.rutaMain, "POH_" + fechor);
            fileTXTd = Path.Combine(DatosGenerales.rutaMain, "POD_" + fechor);

            try
            {

                LogUtil.Graba_Log("ORDEN", "****** INICIO PROCESO PURCHASE *******");

                if (Obtiene_Purchase())
                {
                    //if (Genera_FileTXT())
                    //{
                        if (Envia_FTP())
                        {
                            if (Actualiza_Flag_Purchase())
                            {
                                exito = true;
                            }
                        }
                    //}

                }

                Archiva_TXT();

                if (exito)
                {
                    LogUtil.Graba_Log("ORDEN","PROCESO PURCHASE OK"); // OJO POR MIENTRAS
                }
                else
                {
                    LogUtil.Graba_Log("ORDEN","PROCESO PURCHASE NADA"); // OJO POR MIENTRAS
                }


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log("ORDEN","ERROR: " + ex.ToString());
            }
            finally
            {
                LogUtil.Graba_Log("ORDEN","******** FIN PROCESO PURCHASE *********");
            }
        }
        //****************************************************************************



        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Purchase()
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

                        wnro_ocompra = ds.Tables[0].Rows[ix][1].ToString();

                        wsql = "UPDATE TOCOMPRACX SET FLG_TXWMS = 'X' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
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




        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP()
        {
            bool exito1 = false;
            bool exito2 = false;

            exito1 = Send_FTP_WMS(fileTXTc, fileTXTc);
            exito2 = Send_FTP_WMS(fileTXTd, fileTXTd);

            if (exito1 && exito2) LogUtil.Graba_Log("ORDEN","ENVIA FTP PURCHASE OK "); else LogUtil.Graba_Log("ORDEN","ENVIA FTP PURCHASE ERROR ");
            return (exito1 && exito2);
        }


        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT()
        {

            bool exito1 = false;

            if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
            if (File.Exists(fileTXTd)) File.Delete(fileTXTd);


            return (exito1);
        }


        public bool Obtiene_Purchase()
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

            sql_tocompracx = "SELECT NRO_PROFORM AS LLAVE01, NRO_OCOMPRA AS LLAVE02, '50001' AS CD, '" + codcia +
                                      "' AS EMPRESA, COD_PROVE, 'CREATE' AS ACCION, TO_CHAR(FEC_EMISION,'YYYYMMDD') AS FECHA " +
                                      ", '' AS BLANCO01 " +
                                      ", 'NAC' AS TIPO " +
                                      ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                      ", '' AS BLANCO02 " +
                                      ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                      ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                      ", COD_MONEDA " +
                                      "FROM TOCOMPRACX " +
                                      "WHERE COD_SECCI NOT IN ('M','V') " +
                                      "AND FLG_TXWMS != 'X' " +
                                      "AND NRO_OCOMPRA != '' ";
                                      
                                      //"AND FEC_EMISION > CURRENT_DATE -10 AND FEC_EMISION <= CURRENT_DATE ";

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

            // Grabando registro.
            String[] texto;
            texto = new string[ds.Tables[0].Rows.Count + 1];

            //Rellenamos el detalle del fichero
            String linea;
            //StringBuilder linea2 = new StringBuilder();
            for (int ix = 0; ix < ds.Tables[0].Rows.Count; ix++)
            {
                linea = String.Empty;
                for (int j = 0; j < ds.Tables[0].Columns.Count - 1; j++)
                {
                    linea += ds.Tables[0].Rows[ix][j].ToString() + "|";
                }
                texto[ix] = linea;
                //linea2.Append(linea);
            }


            // Grabando registro al Archivo de Texto CABECERA
            //linea2.Append("\r\n");
            File.WriteAllLines(fileTXTc, texto);





            //-----------------------
            //----- DETALLE ---------
            //-----------------------

            sql_tocompradx = "SELECT TD.NRO_PROFORM AS LLAVE01 " +
                                      ", TD.NRO_OCOMPRA AS LLAVE02 " +
                                      ", '50001' AS CD, '" +
                                      codcia + "' AS EMPRESA " +
                                      ", TD.NRO_ITEM " +
                                      ", 'CREATE' AS TIPO " +
                                      ", TD.COD_PRODUCTO || TD.COD_CALID AS ITEM " +
                                      ", TD.COD_SECCI " +
                                      ", TD.COD_CPACK " +
                                      ", TD.CAN_MED00 AS CANTIDAD " +
                                      ", TD.VAL_PRECIO " +
                                      ", TD.COD_CADENAD " +
                                      ", TD.COD_ALMAC " +
                                      ", TD.CAN_MED00, TD.CAN_MED01, TD.CAN_MED02, TD.CAN_MED03, TD.CAN_MED04, TD.CAN_MED05, TD.CAN_MED06, TD.CAN_MED07, TD.CAN_MED08, TD.CAN_MED09, TD.CAN_MED10, TD.CAN_MED11 " +
                                      "FROM TOCOMPRADX TD " +
                                      "INNER JOIN TOCOMPRACX TC ON TD.NRO_OCOMPRA = TC.NRO_OCOMPRA " +
                                      "WHERE TC.COD_SECCI NOT IN ('M','V') " +
                                      "AND TC.FLG_TXWMS != 'X' " +
                                      "AND TC.NRO_OCOMPRA != '' ";
                                      
                                      //"AND TC.FEC_EMISION > CURRENT_DATE -10 AND TC.FEC_EMISION <= CURRENT_DATE ORDER BY TD.NRO_PROFORM ";

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
            dtdet = Dt_pivot(dtdet);
            StringBuilder str = new StringBuilder();
            int wcorrelativo = 0;
            string worden = "";


            worden = "";

            foreach (DataRow fila in dtdet.Rows)
            {
                string valor;
                string cad_envio = "";
                string moneda = "";


                valor = fila["cod_cpack"].ToString();
                moneda = ds.Tables[0].Rows[0][13].ToString(); // Asignamos Moneda


                if (fila["item"].ToString().Length < 10)
                {
                    string val = fila["item"].ToString();
                }

                if (valor == "00001")// No es Pre-Pack
                {

                    if (fila["cantidad"].ToString() != "000")
                    {

                        if (fila["llave01"].ToString() != worden)
                        {
                            wcorrelativo = 0;
                            worden = fila["llave01"].ToString();
                        }

                        wcorrelativo = wcorrelativo + 1;
                        string numero_formateado = wcorrelativo.ToString("000");

                        cad_envio = fila["llave01"].ToString() + "|" + fila["llave02"].ToString() + "|" + fila["cd"].ToString() + "|" + fila["empresa"].ToString() + "|" + numero_formateado + "|" + fila["tipo"].ToString() +
                                            "|" + fila["item"].ToString().Substring(0, 10) + fila["cod_secci"].ToString() +
                                            "|" + fila["item"].ToString().Substring(0, 7) +
                                            "|" + fila["item"].ToString().Substring(7, 1) +
                                            "|" + fila["item"].ToString().Substring(8, 2) +
                                            "|" + fila["cod_secci"].ToString() +
                                            "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["cantidad"].ToString() +
                                            "|" + fila["val_precio"].ToString() +
                                            "|" + "|" + "|" + "|" +
                                            "0" + "|" +
                                            moneda +
                                            "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|";

                        str.Append(cad_envio);
                        str.Append("\r\n");

                    }



                }
                else // Es Pre-Pack
                {
                    if (fila["cantidad"].ToString() != "000")
                    {
                        wcorrelativo = wcorrelativo + 1;
                        string numero_formateado = wcorrelativo.ToString("000");


                        cad_envio = fila["llave01"].ToString() + "|" + fila["llave02"].ToString() + "|" + fila["cd"].ToString() + "|" + fila["empresa"].ToString() + "|" + numero_formateado + "|" + fila["tipo"].ToString() +
                                            "|" + fila["item"].ToString().Substring(0, 8) + fila["cod_cpack"].ToString() + fila["cod_secci"].ToString() +
                                            "|" + fila["item"].ToString().Substring(0, 7) +
                                            "|" + fila["item"].ToString().Substring(7, 1) +
                                            "|" + fila["item"].ToString().Substring(8, 2) +
                                            "|" + fila["cod_secci"].ToString() +
                                            "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["cantidad"].ToString() +
                                            "|" + fila["val_precio"].ToString() +
                                            "|" + "|" + "|" + "|" +
                                            "0" + "|" +
                                            moneda +
                                            "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                            fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|";

                        str.Append(cad_envio);
                        str.Append("\r\n");
                    }
                }
            }

            string str2 = str.ToString();

            // Grabando registro al Archivo de Texto DETALLE
            File.WriteAllText(fileTXTd, str2);

            { exito = true; }

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
                LogUtil.Graba_Log("ORDEN","ERROR AL SUBIR PURCHASE FTP: " + ex.Message);
            }


            return exito;
        }



        private string Right(string param, int length)
        {
            //start at the index based on the lenght of the sting minus
            //the specified lenght and assign it a variable
            string result = param.Substring(param.Length - length, length);
            //return the result of the operation
            return result;
        }



        private DataTable Dt_pivot(DataTable dt)
        {
            DataTable tmp = dt;
            DataTable dt_tabla = new DataTable();
            if (tmp.Rows.Count > 0 && tmp.Columns.Count > 1)
            {
                int index = 1;
                DataRow dtRow;

                dt_tabla.Columns.Add("llave01", typeof(string));
                dt_tabla.Columns.Add("llave02", typeof(string));
                dt_tabla.Columns.Add("cd", typeof(string));
                dt_tabla.Columns.Add("empresa", typeof(string));
                dt_tabla.Columns.Add("dord_secue", typeof(string));
                dt_tabla.Columns.Add("tipo", typeof(string));
                dt_tabla.Columns.Add("item", typeof(string));
                dt_tabla.Columns.Add("cod_secci", typeof(string));
                dt_tabla.Columns.Add("cod_cpack", typeof(string));
                dt_tabla.Columns.Add("cantidad", typeof(string));
                dt_tabla.Columns.Add("val_precio", typeof(string));
                dt_tabla.Columns.Add("cod_cadenad", typeof(string));
                dt_tabla.Columns.Add("cod_almac", typeof(string));

                for (int i = 13; i < tmp.Columns.Count; i++)
                {
                    DataColumn dc = tmp.Columns[i];
                    string clname = dc.ColumnName;
                    for (int y = 0; y < tmp.Rows.Count; y++)
                    {
                        dtRow = dt_tabla.NewRow();
                        dtRow[0] = tmp.Rows[y][0].ToString();
                        dtRow[1] = tmp.Rows[y][1].ToString();

                        //dtRow[2] = tmp.Rows[y][2].ToString();
                        string wcd = tmp.Rows[y][12].ToString();
                        dtRow[2] = DatosGenerales.Obt_CDxAlm(wcd);


                        dtRow[3] = tmp.Rows[y][3].ToString();
                        dtRow[4] = tmp.Rows[y][4].ToString();
                        dtRow[5] = tmp.Rows[y][5].ToString();

                        string wcantidad;
                        int valor;

                        valor = Convert.ToInt32(tmp.Rows[y][i]);

                        if (valor == 0)
                        {
                            wcantidad = "000";
                        }
                        else
                        {
                            wcantidad = Convert.ToString(tmp.Rows[y][i]);
                        }

                        dtRow[6] = tmp.Rows[y][6].ToString() + Right(clname, 2) + wcantidad;
                        dtRow[7] = tmp.Rows[y][7].ToString();
                        dtRow[8] = tmp.Rows[y][8].ToString();
                        dtRow[9] = wcantidad;
                        dtRow[10] = tmp.Rows[y][10].ToString();
                        dtRow[11] = tmp.Rows[y][11].ToString();
                        dtRow[12] = tmp.Rows[y][12].ToString();

                        dt_tabla.Rows.Add(dtRow);
                        index++;
                    }
                }
            }
            return dt_tabla;
        }
    }
}