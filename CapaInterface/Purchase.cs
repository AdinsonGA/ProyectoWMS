using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WinSCP;
using CapaDatos;
using System.Data;
using System.IO;
using Npgsql;
using System.Configuration;

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
        DataSet ds_det = null;
        DataSet ds_cab = null;

        string Interface = "PURCHASE";

        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

        string fileTXTc_Cho = "";
        string fileTXTc_Lur = "";

        string fileTXTd_Cho = ""; //CD Chorrillos
        string fileTXTd_Lur = ""; //Cd Lurin
        Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);


        public void Genera_Interface_Purchase()
        {
            //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("WMS");

            string wcd = "";
            bool exito = false;
            //************** File de texto

            fileTXTc_Cho = Path.Combine(Crear_Carpetas.WORK, "POH_50001_" + fechor);
            fileTXTc_Lur = Path.Combine(Crear_Carpetas.WORK, "POH_50003_" + fechor);

            fileTXTd_Cho = Path.Combine(Crear_Carpetas.WORK, "POD_50001_" + fechor);
            fileTXTd_Lur = Path.Combine(Crear_Carpetas.WORK, "POD_50003_" + fechor);

            try
            {

                LogUtil.Graba_Log(Interface, "****** INICIO PROCESO PURCHASE *******", false, "");


                if (Obtiene_Purchase())
                {
                    //return;
                    for (int xi = 1; xi <= 2; xi++)
                    {
                        if (xi == 1)
                            wcd = "50001";
                        else
                            wcd = "50003";

                        if (Envia_FTP(wcd))
                        {
                            if (Actualiza_Flag_Purchase())
                            {
                                Archiva_TXT(wcd); // Si todo salio, Movemos de carpeta WORD a BACKUP
                                exito = true;
                            }
                        }
                    }
                }


                if (exito)
                {
                    LogUtil.Graba_Log(Interface, "PROCESO PURCHASE OK", false, "");
                }
                else
                {
                    LogUtil.Graba_Log(Interface, "NO EXISTE DATA PARA PROCESAR", false, "");
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, "Genera_Interface_Purchase");
            }
            finally
            {
                LogUtil.Graba_Log(Interface, "******** FIN PROCESO PURCHASE *********", false, "");
            }
        }
        //****************************************************************************



        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las Ordenes (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Purchase()
        {
            bool exito = false;

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
            {
                try
                {
                    if (cn.State == 0) cn.Open();
                    // Actualiza flag CABECERA - Postgres
                    for (int ix = 0; ix < ds_cab.Tables[0].Rows.Count; ix++)
                    {
                        string wnro_ocompra = "";
                        string wsql = "";

                        wnro_ocompra = ds_cab.Tables[0].Rows[ix][1].ToString();
                    
                        wsql = "UPDATE TOCOMPRACX SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
                        using (NpgsqlCommand cmd = new NpgsqlCommand(wsql, cn))
                        {
                            cmd.CommandTimeout = 5 * 60; // 5 minutos
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, "Actualiza_Flag_Purchase");
                    return false;
                }
                finally
                {
                    cn.Close();

                }
            }
            

            exito = true;
            return exito;
        }

        /**/
        //using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
        //{
        //    try
        //    {
        //        cn.Open();

        //        for (int ix = 0; ix < ds_cab.Tables[0].Rows.Count; ix++)
        //        {
        //            string wnro_ocompra = "";
        //            //string wsql = "";

        //            wnro_ocompra = ds_cab.Tables[0].Rows[ix][1].ToString();
        //            string query = "UPDATE TOCOMPRACX SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
        //            var cmd = new NpgsqlCommand(query, cn);
        //            cmd.ExecuteNonQuery();
        //        }

        //    }
        //    catch (Exception)
        //    {
        //        throw;
        //    }
        //}
        /**/






        /************** Envia_FTP ************
        * Metodo que envia el archivo de texto al FTP
        **************************************/
        private bool Envia_FTP(string wcd)
        {
            bool exito1, exito2, exito5, exito6 = false;
            int cont = 0;

            if (wcd == "50001")
            {
                /* Envia Cabecera */
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Cho)))
                {
                    exito1 = Send_FTP_WMS(fileTXTc_Cho, "/Peru/730/50001/input/" + Path.GetFileName(fileTXTc_Cho), "50001");
                    cont = cont + 1;
                }

                /* Envia Detalle */
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTd_Cho)))
                {
                    exito2 = Send_FTP_WMS(fileTXTd_Cho, "/Peru/730/50001/input/" + Path.GetFileName(fileTXTd_Cho), "50001");
                }
            }


            if (wcd == "50003")
            {
                /* Envia Cabecera */
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Lur)))
                {
                    exito5 = Send_FTP_WMS(fileTXTc_Lur, "/Peru/730/50003/input/" + Path.GetFileName(fileTXTc_Lur), "50003");
                    cont = cont + 1;
                }

                /* Envia Detalle */
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTd_Lur)))
                {
                    exito6 = Send_FTP_WMS(fileTXTd_Lur, "/Peru/730/50003/input/" + Path.GetFileName(fileTXTd_Lur), "50003");
                }
            }



            if (cont >= 1 && wcd == "50001")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL PURCHASE PARA ALMACEN 50001", false, "");
                return true;
            }
            else if (cont >= 1 && wcd == "50003")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL PURCHASE PARA ALMACEN 50003", false, "");
                return true;
            }
            else return false;
        }


        public bool Obtiene_Purchase()
        {
            bool exito = false;

            //-----------------------
            //------ CABECERA -------
            //-----------------------
            Procesa_Data_Cab("C"); // Solo Chorrillos
            Procesa_Data_Cab("L"); // Solo Lurin


            //-----------------------
            //----- DETALLE ---------
            //-----------------------
            Procesa_Data_Det("C"); // Solo Chorrillos
            Procesa_Data_Det("L"); // Solo Lurin
            { exito = true; }

            return exito;
        }

        private void Archiva_TXT(string CodAlmacen)
        {
            try
            {

                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(CodAlmacen);

                if (CodAlmacen == "50001") //cabecera
                {
                    if (File.Exists(fileTXTc_Cho)) //CHORRILLOS
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho));
                        File.Move(fileTXTc_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho)); // Try to move
                    }

                    if (File.Exists(fileTXTd_Cho))//detalle
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho));
                        File.Move(fileTXTd_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho)); // Try to move
                    }
                }

                if (CodAlmacen == "50003")
                {
                    if (File.Exists(fileTXTc_Lur)) //LURIN
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur));
                        File.Move(fileTXTc_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur)); // Try to move
                    }

                    if (File.Exists(fileTXTd_Lur))//detalle
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur));
                        File.Move(fileTXTd_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur)); // Try to move
                    }
                }
            }
            catch
            {
                LogUtil.Graba_Log(Interface, "Error de Envio, Archivo: " + Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur), false, "Archiva_TXT");
                // omitido
            }
        }

        private bool Send_FTP_WMS(string file_origen, string file_destino, string modo)
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
                    PortNumber = Convert.ToInt32(DatosGenerales.PuertoFtp),
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

                    if (modo == "50001")
                    {
                        transferResult = session.PutFiles(file_origen, "/Peru/730/50001/input/" + Path.GetFileName(file_destino), false, transferOptions);
                    }
                    else
                    {
                        transferResult = session.PutFiles(file_origen, "/Peru/730/50003/input/" + Path.GetFileName(file_destino), false, transferOptions);
                    }

                    // Throw on any error
                    transferResult.Check();
                    exito = transferResult.IsSuccess;

                }
            }

            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR AL SUBIR PURCHASE AL FTP: " + ex.Message, true, "Send_FTP_WMS");
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

            try
            {
                if (tmp.Rows.Count > 0 && tmp.Columns.Count > 1) // Barre todos los registros
                {
                    int index = 1;
                    DataRow dtRow;

                    dt_tabla.Columns.Add("llave01", typeof(string));        //0
                    dt_tabla.Columns.Add("llave02", typeof(string));        //1
                    dt_tabla.Columns.Add("cd", typeof(string));             //2  
                    dt_tabla.Columns.Add("empresa", typeof(string));        //3
                    dt_tabla.Columns.Add("dord_secue", typeof(string));     //4
                    dt_tabla.Columns.Add("tipo", typeof(string));           //5
                    dt_tabla.Columns.Add("item", typeof(string));           //6
                    dt_tabla.Columns.Add("cod_secci", typeof(string));      //7
                    dt_tabla.Columns.Add("cod_cpack", typeof(string));      //8
                    dt_tabla.Columns.Add("cantidad", typeof(string));       //9
                    dt_tabla.Columns.Add("val_precio", typeof(string));    //10
                    dt_tabla.Columns.Add("cod_cadenad", typeof(string));    //11
                    dt_tabla.Columns.Add("cod_almac", typeof(string));      //12
                    dt_tabla.Columns.Add("can_ppack", typeof(string));      //13

                    string wcodigo_ant = "";

                    for (int i = 13; i < tmp.Columns.Count - 2; i++) // Solo cogemos las columnas donde van las tallas (12 columnas), las que vienen despues NO!
                    {
                        DataColumn dc = tmp.Columns[i];
                        string clname = dc.ColumnName;
                        dtRow = dt_tabla.NewRow();

                        for (int y = 0; y < tmp.Rows.Count; y++)
                        {

                            dtRow = dt_tabla.NewRow();
                            dtRow[0] = tmp.Rows[y][0].ToString();           //Llave01
                            dtRow[1] = tmp.Rows[y][1].ToString();           //Llave02
                            dtRow[2] = tmp.Rows[y][2].ToString();           //Cd
                            dtRow[3] = tmp.Rows[y][3].ToString();           //Empresa
                            dtRow[4] = tmp.Rows[y][4].ToString();           //dord_secue
                            dtRow[5] = tmp.Rows[y][5].ToString();           //Tipo

                            string wcantidad;
                            Int32 wcan_cpack;
                            int valor;

                            if (tmp.Rows[y][i] == null)
                            {
                                valor = 0;
                            }
                            else
                            {
                                valor = Convert.ToInt32(tmp.Rows[y][i]);
                            }

                            if (valor == 0 && valor == 0.0000)
                            {
                                wcantidad = "000";
                            }
                            else
                            {
                                wcantidad = Convert.ToString(tmp.Rows[y][i]);
                            }


                            // Si NO es Codigo PrePack, se concatena la medida
                            if (tmp.Rows[y][8].ToString() == "00001")
                            {
                                dtRow[6] = tmp.Rows[y][6].ToString() + Right(clname, 2); // Item,
                                wcodigo_ant = "";
                            }
                            // Si es Codigo PrePack, NO se concatena la medida, queda Item + Calidad
                            else
                            {
                                dtRow[6] = tmp.Rows[y][6].ToString();                   // Item
                                wcodigo_ant = tmp.Rows[y][6].ToString() + tmp.Rows[y][8].ToString() + tmp.Rows[y][7].ToString();        // capturamos el codigo del item para resumir
                            }


                            //dtRow[7] = tmp.Rows[y][7].ToString();                       // cod_secci
                            dtRow[7] = "";                                              // cod_secci
                            dtRow[8] = tmp.Rows[y][8].ToString();                       // cod_cpack


                            // Si NO es Codigo PrePack, se concatena la medida
                            if (tmp.Rows[y][8].ToString() == "00001")
                            {
                                dtRow[9] = wcantidad;                                   // cantidad por medida
                                dtRow[10] = tmp.Rows[y][10].ToString();                     // val_precio
                            }
                            // Si es Codigo PrePack, NO se concatena la medida, queda Item + Calidad
                            else
                            {
                                dtRow[9] = tmp.Rows[y][26].ToString();                  // cantidad por ppack
                                if (tmp.Rows[y][8].ToString() == null)
                                {
                                    wcan_cpack = 0;
                                }
                                else
                                {
                                    wcan_cpack = Convert.ToInt32(tmp.Rows[y][8].ToString().Substring(0, 2));
                                }

                                dtRow[10] = Convert.ToDouble(tmp.Rows[y][10].ToString()) * wcan_cpack;                     // val_precio

                            }

                            dtRow[11] = tmp.Rows[y][11].ToString();                     // cod_cadenad
                            dtRow[12] = tmp.Rows[y][12].ToString();                     // cod_almac
                            dtRow[13] = tmp.Rows[y][26].ToString();                     // can_ppack

                            if (wcodigo_ant.Length < 14) // No es Cod Prepack
                            {
                                dt_tabla.Rows.Add(dtRow);
                            }
                            else
                            {
                                if (i == 13)
                                {
                                    dt_tabla.Rows.Add(dtRow);
                                }
                            }
                            index++;
                        }//for

                    }//for
                }//Iif
            }
            catch (Exception)
            {
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas", true, "Dt_pivot");
            }

            return dt_tabla;
        }//private


        private DataTable Procesa_Data_Det(string modo)
        {


            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompradx = null;
            ds_det = new DataSet();

            string sql_tocompradx, wfiltro = "", wCD = "";
            string codcia = "730";

            if (modo == "C")            // Es Chorillos
            {
                wfiltro = " (LENGTH(CA.DES_CDS) > 5 OR CA.DES_CDS = '50001') ";
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
            {
                wfiltro = " (LENGTH(CA.DES_CDS) > 5 OR CA.DES_CDS = '50003') ";
                wCD = "50003";
            }


            try
            {
                sql_tocompradx = "SELECT TD.NRO_PROFORM AS LLAVE01 " +
                                          ", TD.NRO_OCOMPRA AS LLAVE02 " +
                                          ", " + wCD + " AS CD, '" +
                                          codcia + "' AS EMPRESA " +
                                          ", MAX(TD.NRO_ITEM) AS NRO_ITEM " +
                                          ", 'CREATE' AS TIPO " +
                                          //", 'UPDATE' AS TIPO " +
                                          ", TD.COD_PRODUCTO || TD.COD_CALID AS ITEM " +
                                          ", MAX(CASE WHEN TD.COD_SECCI IS NULL THEN '' ELSE TD.COD_SECCI END) AS COD_SECCI " +
                                          ", CASE WHEN TD.COD_CPACK IS NULL THEN '' ELSE TD.COD_CPACK END AS COD_CPACK " +
                                          ", SUM(TD.CAN_MED00) AS CANTIDAD " +
                                          ", MAX(TD.VAL_PRECIO) AS VAL_PRECIO " +
                                          ", '' AS COD_CADENAD " +
                                          ", MAX(SUBSTRING(TG.DES_CAMPO2, 1, 1)) AS COD_ALMAC " +
                                          ", SUM(CASE WHEN TD.CAN_MED00 IS NULL THEN 0 ELSE TD.CAN_MED00 END) AS CAN_MED01 " +
                                          ", SUM(CASE WHEN TD.CAN_MED01 IS NULL THEN 0 ELSE TD.CAN_MED01 END) AS CAN_MED02 " +
                                          ", SUM(CASE WHEN TD.CAN_MED02 IS NULL THEN 0 ELSE TD.CAN_MED02 END) AS CAN_MED03 " +
                                          ", SUM(CASE WHEN TD.CAN_MED03 IS NULL THEN 0 ELSE TD.CAN_MED03 END) AS CAN_MED04 " +
                                          ", SUM(CASE WHEN TD.CAN_MED04 IS NULL THEN 0 ELSE TD.CAN_MED04 END) AS CAN_MED05 " +
                                          ", SUM(CASE WHEN TD.CAN_MED05 IS NULL THEN 0 ELSE TD.CAN_MED05 END) AS CAN_MED06 " +
                                          ", SUM(CASE WHEN TD.CAN_MED06 IS NULL THEN 0 ELSE TD.CAN_MED06 END) AS CAN_MED07 " +
                                          ", SUM(CASE WHEN TD.CAN_MED07 IS NULL THEN 0 ELSE TD.CAN_MED07 END) AS CAN_MED08 " +
                                          ", SUM(CASE WHEN TD.CAN_MED08 IS NULL THEN 0 ELSE TD.CAN_MED08 END) AS CAN_MED09 " +
                                          ", SUM(CASE WHEN TD.CAN_MED09 IS NULL THEN 0 ELSE TD.CAN_MED09 END) AS CAN_MED10 " +
                                          ", SUM(CASE WHEN TD.CAN_MED10 IS NULL THEN 0 ELSE TD.CAN_MED10 END) AS CAN_MED11 " +
                                          ", SUM(CASE WHEN TD.CAN_MED11 IS NULL THEN 0 ELSE TD.CAN_MED11 END) AS CAN_MED12 " +
                                          ", MAX(CASE WHEN CA.DES_CDS IS NULL THEN '' ELSE CA.DES_CDS END) AS DES_CDS " +
                                          ", SUM(CASE WHEN TD.CAN_PPACK IS NULL THEN 0 ELSE TD.CAN_PPACK END) AS CAN_PPACK " +
                                          "FROM TOCOMPRADX TD " +
                                          "INNER JOIN TOCOMPRACX TC ON TD.NRO_OCOMPRA = TC.NRO_OCOMPRA " +
                                          "INNER JOIN TCADENA CA ON TD.COD_CADENAD = CA.COD_CADENA " +
                                          "INNER JOIN TGENERALD TG ON TG.COD_TABLA='056' AND TD.COD_CADENAD = TG.DES_CAMPO4 AND " + wCD + "|| 'P' = TG.DES_CAMPO5 " +
                                          "WHERE TC.COD_SECCI NOT IN ('M','V') " +                     // NO CONSIDERAR (MATERIALES, VARIOS)
                                          "AND TC.FLG_TXWMS != '1' " +                                  // SOLO LAS ORDENES PENDIENTES
                                          "AND " + wfiltro +
                                          "AND TC.TIP_ESTAD NOT IN('D','C','K') " +                     // NO CONSIDERAR (DESACTIVOS, COMPLETOS, CANCELADOS), CABECERA DE ORDEN
                                          "AND TD.TIP_ESTAD NOT IN ('D','T','K') " +                    // NO CONSIDERAR (DESACTIVOS, COMPLETOS, CANCELADOS), DETALLE DE ORDEN
                                          "AND LENGTH(TD.COD_PRODUCTO) = 7 " +
                                          "AND TC.COD_EMPRESA = '02' " +                                // SOLO PERU
                                          "AND TC.FEC_EMISION >= '20170101' " +                         // A PARTIR DE ENE - 2017
                                          "AND TRIM(TC.NRO_OCOMPRA) != '' " +                           // SOLO ORDENES QUE TENGAN ORDEN DE COMPRA
                                          "AND CURRENT_DATE - TC.FEC_EMISION <= " + Dias +
                                          "GROUP BY TD.NRO_PROFORM, TD.NRO_OCOMPRA, TD.COD_PRODUCTO, TD.COD_CALID, TD.COD_CPACK ";


                using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
                {
                    /*selecccionando el archivo TOCOMPRADX */
                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompradx, cn))
                    {
                        cmd.CommandTimeout = 5 * 60; // 5 minutos
                        using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            dt_tocompradx = new DataTable();
                            da.Fill(dt_tocompradx);
                        }
                        dt_tocompradx.TableName = "TOCOMPRADX";
                    }

                    cn.Close();
                }

                if ((dt_tocompradx != null && dt_tocompradx.Rows.Count > 0))
                {
                    ds_det.Tables.Add(dt_tocompradx);

                    DataTable dtdet = ds_det.Tables[0];

                    /* Metodo que invierte las columnas a Filas */
                    dtdet = Dt_pivot(dtdet);

                    dtdet.DefaultView.Sort = "llave02";
                    dtdet = dtdet.DefaultView.ToTable();



                    int wcorrelativo = 0;
                    string worden = "";
                    StringBuilder str = new StringBuilder();



                    foreach (DataRow fila in dtdet.Rows)
                    {
                        string valor;
                        string cad_envio = "";
                        string moneda = "";


                        valor = fila["cod_cpack"].ToString();
                        moneda = ds_det.Tables[0].Rows[0][13].ToString(); // Asignamos Moneda


                        if (fila["item"].ToString().Length < 10)
                        {
                            string val = fila["item"].ToString();
                        }

                        // Inicializamos el correlativo
                        if (fila["llave01"].ToString() != worden)
                        {
                            wcorrelativo = 0;
                            worden = fila["llave01"].ToString();
                        }


                        if (valor == "00001")// No es Pre-Pack
                        {

                            if (fila["cantidad"].ToString() != "000")
                            {

                                wcorrelativo = wcorrelativo + 1;
                                string numero_formateado = wcorrelativo.ToString("000");

                                cad_envio = fila["llave01"].ToString() + "|" +                                              //0  hdr_group_nbr
                                            fila["llave02"].ToString() + "|" +                                              //1  po_nbr
                                            fila["cd"].ToString() + "|" +                                                   //2  facility_code
                                            fila["empresa"].ToString() + "|" +                                              //3  company_code
                                            numero_formateado + "|" +                                                       //4  seq_nbr
                                            fila["tipo"].ToString() + "|" +                                                 //5  action_code
                                                                                                                            //fila["item"].ToString() + fila["cod_secci"].ToString() + "|" +                //6  item_alternate_code
                                            fila["item"].ToString() + "|" +                                                 //6  item_alternate_code
                                            fila["item"].ToString().Substring(0, 7) + "|" +                                 //7  item_part_a
                                            fila["item"].ToString().Substring(7, 1) + "|" +                                 //8  item_part_b
                                            fila["item"].ToString().Substring(8, 2) + "|" +                                 //9  item_part_c
                                                                                                                            //fila["cod_secci"].ToString() + "|" +                                          //10 item_part_d
                                            "|" +                                                                           //10 item_part_d
                                            "|" +                                                                           //11 item_part_e
                                            "|" +                                                                           //12 item_part_f
                                            "|" +                                                                           //13 pre_pack_code
                                            "|" +                                                                           //14 pre_pack_ratio
                                            "|" +                                                                           //15 pre_pack_total_units
                                            fila["cantidad"].ToString() + "|" +                                             //16 ord_qty
                                            fila["val_precio"].ToString() + "|" +                                           //17 unit_cost
                                            "|" +                                                                           //18 vendor_item_code
                                            "|" +                                                                           //19 internal_misc_n1
                                            "|" +                                                                           //20 internal_misc_a1
                                            "0" + "|" +                                                                     //21 unit_retail
                                            fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|" +           //22 cust_field_1
                                            "|" +                                                                           //23 
                                            "|" +                                                                           //24 
                                            "|" +                                                                           //25 
                                            "|" +                                                                           //26 
                                            "|" +                                                                           //27 
                                            "|" +                                                                           //28 
                                            "|" +                                                                           //29 
                                            "|" +                                                                           //30 
                                            "|" +                                                                           //31 
                                            "|";                                                                           //32 


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

                                cad_envio = fila["llave01"].ToString() + "|" +                                                                      //0  hdr_group_nbr
                                            fila["llave02"].ToString() + "|" +                                                                      //1  po_nbr
                                            fila["cd"].ToString() + "|" +                                                                           //2  facility_code
                                            fila["empresa"].ToString() + "|" +                                                                      //3  company_code
                                            numero_formateado + "|" +                                                                               //4  seq_nbr
                                            fila["tipo"].ToString() + "|" +                                                                         //5  action_code
                                                                                                                                                    //fila["item"].ToString() + fila["cod_cpack"].ToString() + fila["cod_secci"].ToString() + "|" +         //6  item_alternate_code
                                            fila["item"].ToString() + fila["cod_cpack"].ToString() + "|" +                                          //6  item_alternate_code
                                            fila["item"].ToString().Substring(0, 7) + "|" +                                                         //7  item_part_a
                                            fila["item"].ToString().Substring(7, 1) + "|" +                                                         //8  item_part_b
                                            fila["cod_cpack"].ToString() + "|" +                                                                    //9  item_part_c
                                                                                                                                                    //fila["cod_secci"].ToString() + "|" +                                                                  //10 item_part_d
                                            "|" +                                                                                                   //10 item_part_d
                                            "|" +                                                                                                   //11 item_part_e
                                            "|" +                                                                                                   //12 item_part_f
                                            "|" +                                                                                                   //13 pre_pack_code
                                            "|" +                                                                                                   //14 pre_pack_ratio
                                            "|" +                                                                                                   //15 pre_pack_total_units
                                            fila["cantidad"].ToString() + "|" +                                                                     //16 ord_qty
                                            fila["val_precio"].ToString() + "|" +                                                                   //17 unit_cost
                                            "|" +                                                                                                   //18 vendor_item_code
                                            "|" +                                                                                                   //19 internal_misc_n1
                                            "|" +                                                                                                   //20 internal_misc_a1
                                            "0" + "|" +                                                                                             //21 unit_retail
                                            fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|" +                                   //22 cust_field_1
                                            "|" +                                                                                                   //23 
                                            "|" +                                                                                                   //24 
                                            "|" +                                                                                                   //25 
                                            "|" +                                                                                                   //26 
                                            "|" +                                                                                                   //27 
                                            "|" +                                                                                                   //28 
                                            "|" +                                                                                                   //29 
                                            "|" +                                                                                                   //30 
                                            "|" +                                                                                                   //31 
                                            "|";                                                                                                    //32 


                                str.Append(cad_envio);
                                str.Append("\r\n");
                            }
                        }

                    }// End For


                    /*  Generando Archivos de Texto */
                    if (modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Cho, str2);
                    }
                    if (modo == "L")            // Solo Lurin
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Lur, str2);
                    }

                }//if Si tiene registros
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas : " + ex.Message.ToString(), true, "Procesa_Data_Det");
            }
            return dt_tabla;
        }

        private DataTable Procesa_Data_Cab(String modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompracx = null;

            string wfiltro = "", wCD = "";
            string codcia = "730";
            StringBuilder str = new StringBuilder();

            if (modo == "C")            // Es Chorillos
            {
                wfiltro = " (LENGTH(CA.DES_CDS) > 5 OR CA.DES_CDS = '50001') ";
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
            {
                wfiltro = " (LENGTH(CA.DES_CDS) > 5 OR CA.DES_CDS = '50003') ";
                wCD = "50003";
            }

            try
            {

                string sql_tocompracx = "";

                //-----------------------
                //------ CABECERA -------
                //-----------------------

                sql_tocompracx = "SELECT DISTINCT TC.NRO_PROFORM AS LLAVE01 " +
                                ", TC.NRO_OCOMPRA AS LLAVE02 " +
                                ", " + wCD + " AS CD " +
                                ", " + codcia + " AS EMPRESA " +
                                ", COD_PROVE " +
                                ", 'CREATE' AS ACCION " +
                                //", 'UPDATE' AS ACCION " +
                                ", TO_CHAR(FEC_EMISION,'YYYYMMDD') AS FECHA " +
                                ", '' AS BLANCO01 " +
                                ", CASE WHEN TC.TIP_ORIGEN = 'I' THEN 'IMP' ELSE 'NAC' END TIPO " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", '' AS BLANCO02 " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", COD_MONEDA " +
                                "FROM TOCOMPRACX TC " +
                                "INNER JOIN TOCOMPRADX TD ON TC.NRO_OCOMPRA = TD.NRO_OCOMPRA " +
                                "INNER JOIN TCADENA CA ON TD.COD_CADENAD = CA.COD_CADENA " +
                                "WHERE TC.COD_SECCI NOT IN ('M','V') " +                    // NO CONSIDERAR (MATERIALES, VARIOS)
                                "AND TC.FLG_TXWMS != '1' " +                                 // SOLO LAS PENDIENTES DE ENVIO
                                "AND " + wfiltro +
                                "AND TC.TIP_ESTAD NOT IN('D','C','K') " +
                                "AND TD.TIP_ESTAD NOT IN ('D','T','K') " +                  // NO CONSIDERAR (DESACTIVOS, COMPLETOS Y CANCELADOS)
                                "AND LENGTH(TD.COD_PRODUCTO) = 7 " +                        // SOLO LAS ORDENES DE ZAPATOS, ROPAS ETC, NO INCLUYE ECONOMATO OTROS
                                "AND TC.COD_EMPRESA = '02' " +                              // SOLO PERU
                                "AND TC.FEC_EMISION >= '20170101' " +                       // A PARTIR DEL ENER - 2017
                                "AND TRIM(TC.NRO_OCOMPRA) != '' " +                         // SOLO ORDENES QUE TENGAN ORDEN DE COMPRA
                                "AND CURRENT_DATE - TC.FEC_EMISION <= " + Dias;


                //"AND TC.NRO_OCOMPRA IN ('201902594') ";

                using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
                {
                    /*selecccionando el archivo TOCOMPRACX */
                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompracx, cn))
                    {
                        cmd.CommandTimeout = 5 * 60; // 5 minutos
                        using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            dt_tocompracx = new DataTable();
                            da.Fill(dt_tocompracx);

                        }
                        dt_tocompracx.TableName = "TOCOMPRACX";
                    }
                    cn.Close();
                }

                if ((dt_tocompracx != null && dt_tocompracx.Rows.Count > 0))
                {

                    ds_cab = new DataSet();
                    ds_cab.Tables.Add(dt_tocompracx);

                    string delimited = "|";
                    var strCab = new StringBuilder();

                    for (int ix = 0; ix < ds_cab.Tables[0].Rows.Count; ix++)
                    {
                        for (int j = 0; j < ds_cab.Tables[0].Columns.Count - 1; j++)
                        {
                            strCab.Append(ds_cab.Tables[0].Rows[ix][j].ToString() + delimited);    // Numero de orden de despacho
                        }
                        strCab.Append("\r\n");
                    }

                    if (modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_Cho, strCab.ToString());
                    }
                    if (modo == "L")            // Solo Lurin
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_Lur, strCab.ToString());
                    }

                }// if
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, ex.Message, true, "Procesa_Data_Cab");
            }
            return dt_tabla;

        }// Procesa_Data_Cab
    }// Purchase
}// CapaInterface