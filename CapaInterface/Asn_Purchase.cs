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
using System.Data.SqlClient;


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
        string wcodcia = DatosGenerales.codcia;
        DataSet ds = null;
        DataSet dsc = null;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

        string fileTXTc_Cho = "";
        string fileTXTc_Lur = "";

        string fileTXTd_Cho = ""; //CD Chorrillos
        string fileTXTd_Lur = ""; //Cd Lurin
        string Interface = "ASN_PURCHASE";
        Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

        public void Genera_Interface_Asn_Purchase()
        {

            //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("WMS");

            bool exito = false;
            string wcd = "";
            //************** File de texto

            fileTXTc_Cho = Path.Combine(Crear_Carpetas.WORK, "ISH_50001_C_" + fechor);
            fileTXTd_Cho = Path.Combine(Crear_Carpetas.WORK, "ISL_50001_C_" + fechor);

            fileTXTc_Lur = Path.Combine(Crear_Carpetas.WORK, "ISH_50003_L_" + fechor);
            fileTXTd_Lur = Path.Combine(Crear_Carpetas.WORK, "ISL_50003_L_" + fechor);

            try
            {

                LogUtil.Graba_Log(Interface, "****** INICIO PROCESO ASN PURCHASE *******", false, "");
                if (Obtiene_Asn_Purchase())
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
                            if (Actualiza_Flag_Asn_Purchase())
                            {
                                Archiva_TXT(wcd); // Si todo salio, Movemos de carpeta WORD a BACKUP
                                exito = true;
                            }
                        }
                    }
                }

                if (exito)
                {
                    LogUtil.Graba_Log(Interface, "PROCESO ASN PURCHASE OK", false, "");
                }
                else
                {
                    LogUtil.Graba_Log(Interface, "NO EXISTE INFORMACION A PROCESAR", false, "");
                }
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, fileTXTc_Cho);
            }
            finally
            {
                LogUtil.Graba_Log(Interface, "******** FIN PROCESO ASN PURCHASE *********", false, "");
            }
        }
        //****************************************************************************


        public bool Obtiene_Asn_Purchase()
        {
            bool exito = false;

            ////-----------------------
            ////----- DETALLE ---------
            ////-----------------------

            Procesa_Data_Det("C"); // Solo Chorrillos
            Procesa_Data_Det("L"); // Solo Lurin

            ////-----------------------
            ////----- CABECERA---------
            ////-----------------------

            Procesa_Data_Cab("C"); // Solo Chorrillos
            Procesa_Data_Cab("L"); // Solo Lurin

            { exito = true; }

            return exito;
        }

        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP(string wcd)
        {
            bool exito1, exito2, exito5, exito6 = false;
            int cont = 0;

            if (wcd == "50001")
            {
                /* Enviando Cabecera*/
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Cho)))
                {
                    exito1 = Send_FTP_WMS(fileTXTc_Cho, "/Peru/730/50001/input/" + Path.GetFileName(fileTXTc_Cho), "50001");
                    cont = cont + 1;
                }

                /* Enviando Detalle*/
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTd_Cho)))
                {
                    exito2 = Send_FTP_WMS(fileTXTd_Cho, "/Peru/730/50001/input/" + Path.GetFileName(fileTXTd_Cho), "50001");
                }
            }


            if (wcd == "50003")
            {
                /* Enviando Cabecera*/
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Lur)))
                {
                    exito5 = Send_FTP_WMS(fileTXTc_Lur, "/Peru/730/50003/input/" + Path.GetFileName(fileTXTc_Lur), "50003");
                    cont = cont + 1;
                }

                /* Enviando Detalle*/
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTd_Lur)))
                {
                    exito6 = Send_FTP_WMS(fileTXTd_Lur, "/Peru/730/50003/input/" + Path.GetFileName(fileTXTd_Lur), "50003");
                }
            }

            if (cont >= 1 && wcd == "50001")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL ASN PURCHASE PARA ALMACEN 50001", false, "");
                return true;

            }
            else if (cont >= 1 && wcd == "50003")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL ASN PURCHASE PARA ALMACEN 50003", false, "");
                return true;
            }
            else return false;

        }

        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las ASN (para que no lo vuelva a enviar, esto se da en el Postgres)
        ***************/
        private bool Actualiza_Flag_Asn_Purchase()
        {
            bool exito = false;
            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
            {
                try
                {
                    if (cn.State == 0) cn.Open();
                    // Grabando al Postgres CABECERA
                    for (int ix = 0; ix < dsc.Tables[0].Rows.Count; ix++)
                    {
                        string wnro_ocompra = "";
                        string wnro_parcial = "";
                        string wcod_cadenad = "";

                        string wsql = "";

                        // Se evalua campo Llave02 del Detalle del ASN
                        // Para el caso de que el (Numero Parcial) venga vacio, es decir Solo viene el Num de orden
                        if (dsc.Tables[0].Rows[ix][1].ToString().Length == 9)
                        {
                            wnro_ocompra = dsc.Tables[0].Rows[ix][1].ToString(); // Solo Numero de Orden
                            wsql = "UPDATE TOCOMPRAPLX_ASN SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "' AND NRO_PARCIAL = ' ' ";
                        }


                        // Se evalua campo Llave02 del Detalle del ASN
                        // Para el caso de que el (Numero Parcial) venga lleno, los 2 ultimos digitos del campo

                        if (dsc.Tables[0].Rows[ix][1].ToString().Length > 9) //Para el caso de que el Numero Parcial venga lleno
                        {
                            wnro_ocompra = dsc.Tables[0].Rows[ix][1].ToString().Substring(0, 9); // Extraemos el Numero de Orden
                            wnro_parcial = dsc.Tables[0].Rows[ix][1].ToString().Substring(9, 2); // Extraemos el Numero Parcial
                            wcod_cadenad = dsc.Tables[0].Rows[ix][1].ToString().Substring(11, 2); // Extraemos la Cadena

                            wsql = "UPDATE TOCOMPRAPLX_ASN SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "' AND NRO_PARCIAL = '" + wnro_parcial + "' AND COD_CADENAD = '" + wcod_cadenad + "' ";
                        }

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
                    if (cn != null)
                        if (cn.State == ConnectionState.Open) cn.Close();
                    LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, "Actualiza_Flag_Asn_Purchase");
                }
                finally
                {
                    cn.Close();
                }
            }

            exito = true;
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
                    /* Movemos Cabecera */
                    if (File.Exists(fileTXTc_Cho))
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho));
                        File.Move(fileTXTc_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho)); // Try to move
                    }

                    /* Movemos Detalle */
                    if (File.Exists(fileTXTd_Cho))
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho));
                        File.Move(fileTXTd_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho)); // Try to move
                    }
                }

                if (CodAlmacen == "50003")
                {
                    /* Movemos Cabecera */
                    if (File.Exists(fileTXTc_Lur))
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur));
                        File.Move(fileTXTc_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur)); // Try to move
                    }

                    /* Movemos Detalle */
                    if (File.Exists(fileTXTd_Lur))
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur));
                        File.Move(fileTXTd_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur)); // Try to move
                    }
                }
            }
            catch
            {
                LogUtil.Graba_Log(Interface, "Error de Envio, Archivo: " + Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho), false, "");
                LogUtil.Graba_Log(Interface, "Error de Envio, Archivo: " + Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur), false, "");
                // omitido
            }
        }

        private bool Send_FTP_WMS(string file_origen, string file_destino, string modo)
        {
            bool exito = false;
            try
            {
                // Setup session options produccion
                SessionOptions sessionOptions = new SessionOptions
                {

                    Protocol = Protocol.Sftp,
                    HostName = DatosGenerales.UrlFtp,
                    UserName = DatosGenerales.UserFtp,
                    Password = DatosGenerales.PassFtp,
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
                LogUtil.Graba_Log(Interface, "ASN PURCHASE" + " ERROR: " + ex.ToString(), true, "Archiva_TXT");
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

                if (tmp.Rows.Count > 0) //&& tmp.Columns.Count > 1)
                {
                    int index = 1;
                    DataRow dtRow;

                    dt_tabla.Columns.Add("llave01", typeof(string));            //00
                    dt_tabla.Columns.Add("llave02", typeof(string));            //01
                    dt_tabla.Columns.Add("cd", typeof(string));                 //02
                    dt_tabla.Columns.Add("empresa", typeof(string));            //03
                    dt_tabla.Columns.Add("accion", typeof(string));             //04
                    dt_tabla.Columns.Add("tipo", typeof(string));               //05
                    dt_tabla.Columns.Add("item", typeof(string));               //06
                    dt_tabla.Columns.Add("cod_secci", typeof(string));          //07
                    dt_tabla.Columns.Add("cod_cpack", typeof(string));          //08
                    dt_tabla.Columns.Add("cantidad", typeof(string));           //09
                    dt_tabla.Columns.Add("cod_cadenad", typeof(string));        //10
                    dt_tabla.Columns.Add("cod_almacen", typeof(string));        //11
                    dt_tabla.Columns.Add("can_ppack", typeof(string));          //12

                    string wcodigo_ant = "";

                    for (int i = 10; i < tmp.Columns.Count - 3; i++) // Solo cogemos las columnas donde van las tallas (12 columnas), las que vienen despues NO!
                    {
                        DataColumn dc = tmp.Columns[i];
                        string clname = dc.ColumnName;
                        for (int y = 0; y < tmp.Rows.Count; y++)
                        {
                            dtRow = dt_tabla.NewRow();
                            dtRow[0] = tmp.Rows[y][0].ToString(); // Llave01
                            dtRow[1] = tmp.Rows[y][1].ToString(); // Llave02
                            dtRow[2] = tmp.Rows[y][2].ToString(); // Cd
                            dtRow[3] = tmp.Rows[y][3].ToString(); // Empresa
                            dtRow[4] = tmp.Rows[y][4].ToString(); // Accion
                            dtRow[5] = tmp.Rows[y][5].ToString(); // Tipo

                            string wcantidad;
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


                            // Si NO tiene Codigo PrePack, se concatena la medida
                            if (tmp.Rows[y][8].ToString() == "00001")
                            {
                                dtRow[6] = tmp.Rows[y][6].ToString() + Right(clname, 2);
                                wcodigo_ant = "";
                            }
                            // Si tiene Codigo PrePack, NO se concatena la medida, queda Item + Calidad
                            else
                            {
                                dtRow[6] = tmp.Rows[y][6].ToString();
                                wcodigo_ant = tmp.Rows[y][6].ToString() + tmp.Rows[y][8].ToString() + tmp.Rows[y][7].ToString();        // capturamos el codigo del item para resumir
                            }


                            dtRow[7] = "";
                            dtRow[8] = tmp.Rows[y][8].ToString();

                            // Si NO es Codigo PrePack, se concatena la medida
                            if (tmp.Rows[y][8].ToString() == "00001")
                            {
                                dtRow[9] = wcantidad;                                   // cantidad por medida
                            }
                            // Si es Codigo PrePack, NO se concatena la medida, queda Item + Calidad
                            else
                            {
                                dtRow[9] = tmp.Rows[y][24].ToString();                  // cantidad por ppack
                            }


                            dtRow[10] = tmp.Rows[y][22].ToString(); // Cod_cadena
                            dtRow[11] = tmp.Rows[y][23].ToString(); // 
                            dtRow[12] = tmp.Rows[y][24].ToString(); // Can_ppack

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
                }//if
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas (Pivot) ", false, "Dt_pivot");
            }

            return dt_tabla;
        }//private


        private DataTable Procesa_Data_Det(String modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompradx = null;
            ds = new DataSet();

            string wCD = "";

            if (modo == "C")            // Es Chorillos
            {
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
            {
                wCD = "50003";
            }


            try
            {
                string sql_tocompradx = "EXEC [USP_ASN_DETALLE] " + wCD;
                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    /*selecccionando el archivo TOCOMPRADX */
                    using (SqlCommand cmd = new SqlCommand(sql_tocompradx, cn))
                    {
                        cmd.CommandTimeout = 5 * 60; // 5 minutos
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
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
                    ds.Tables.Add(dt_tocompradx);

                    DataTable dtdet = ds.Tables[0];
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

                        valor = fila["cod_cpack"].ToString();

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

                                cad_envio = fila["llave01"].ToString() + "|" +                                                      //0  hdr_group_nbr
                                                    fila["llave02"].ToString() + "|" +                                              //1  shipment_nbr
                                                    fila["cd"].ToString() + "|" +                                                   //2  facility_code
                                                    fila["empresa"].ToString() + "|" +                                              //3  company_code
                                                    numero_formateado + "|" +                                                       //4  seq_nbr
                                                    fila["accion"].ToString() + "|" +                                               //5  action_code
                                                    "|" +                                                                           //6  lpn_nbr
                                                    "|" +                                                                           //7  lpn_weight
                                                    "|" +                                                                           //8  lpn_volume
                                                                                                                                    //fila["item"].ToString() + fila["cod_secci"].ToString() + "|" +                //9  item_alternate_code
                                                    fila["item"].ToString() + "|" +                                                 //9  item_alternate_code
                                                    fila["item"].ToString().Substring(0, 7) + "|" +                                 //10 item_part_a
                                                    fila["item"].ToString().Substring(7, 1) + "|" +                                 //11 item_part_b
                                                    fila["item"].ToString().Substring(8, 2) + "|" +                                 //12 item_part_c
                                                                                                                                    //fila["cod_secci"].ToString() + "|" +                                          //13 item_part_d
                                                    "|" +                                                                           //13 item_part_d
                                                    "|" +                                                                           //14 item_part_e
                                                    "|" +                                                                           //15 item_part_f
                                                    "|" +                                                                           //16 pre_pack_code
                                                    "|" +                                                                           //17 pre_pack_ratio
                                                    "|" +                                                                           //18 pre_pack_total_units
                                                    "|" +                                                                           //19 invn_attr_a
                                                    "|" +                                                                           //20 invn_attr_b
                                                    "|" +                                                                           //21 invn_attr_c
                                                    fila["cantidad"].ToString() + "|" +                                             //22 shipped_qty
                                                    "|" +                                                                           //23 priority_date
                                                    fila["llave02"].ToString().Substring(0, 9) + "|" +                               //24 po_nbr                                            //24 po_nbr
                                                    "|" +                                                                           //25 pallet_nbr
                                                    fila["cod_cadenad"].ToString() + fila["cod_almacen"].ToString() + "|" +         //26 putaway_type
                                                    "|" +                                                                           //27 expiry_date
                                                    "|" +                                                                           //28 batch_nbr
                                                    "|" +                                                                           //29 recv_xdock_facility_code
                                                    "|" +                                                                           //30 cust_field_1
                                                    "|" +                                                                           //31 cust_field_2
                                                    "|";                                                                            //32 cust_field_3

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

                                cad_envio = fila["llave01"].ToString() + "|" +                                                      //0  hdr_group_nbr
                                                    fila["llave02"].ToString() + "|" +                                              //1  shipment_nbr
                                                    fila["cd"].ToString() + "|" +                                                   //2  facility_code
                                                    fila["empresa"].ToString() + "|" +                                              //3  company_code
                                                    numero_formateado + "|" +                                                       //4  seq_nbr
                                                    fila["accion"].ToString() + "|" +                                               //5  action_code
                                                    "|" +                                                                           //6  lpn_nbr
                                                    "|" +                                                                           //7  lpn_weight
                                                    "|" +                                                                           //8  lpn_volume
                                                                                                                                    //fila["item"].ToString() + fila["cod_cpack"].ToString() + fila["cod_secci"].ToString() + "|" + //9  item_alternate_code
                                                    fila["item"].ToString() + fila["cod_cpack"].ToString() + "|" +                  //9  item_alternate_code
                                                    fila["item"].ToString().Substring(0, 7) + "|" +                                 //10 item_part_a
                                                    fila["item"].ToString().Substring(7, 1) + "|" +                                 //11 item_part_b
                                                    fila["cod_cpack"].ToString() + "|" +                                            //12 item_part_c
                                                                                                                                    //fila["cod_secci"].ToString() + "|" +                                            //13 item_part_d
                                                    "|" +                                                                           //13 item_part_d
                                                    "|" +                                                                           //14 item_part_e
                                                    "|" +                                                                           //15 item_part_f
                                                    "|" +                                                                           //16 pre_pack_code
                                                    "|" +                                                                           //17 pre_pack_ratio
                                                    "|" +                                                                           //18 pre_pack_total_units
                                                    "|" +                                                                           //19 invn_attr_a
                                                    "|" +                                                                           //20 invn_attr_b
                                                    "|" +                                                                           //21 invn_attr_c
                                                    fila["cantidad"].ToString() + "|" +                                             //22 shipped_qty
                                                    "|" +                                                                           //23 priority_date
                                                    fila["llave02"].ToString().Substring(0, 9) + "|" +                               //24 po_nbr
                                                    "|" +                                                                           //25 pallet_nbr
                                                    fila["cod_cadenad"].ToString() + fila["cod_almacen"].ToString() + "|" +         //26 putaway_type
                                                    "|" +                                                                           //27 expiry_date
                                                    "|" +                                                                           //28 batch_nbr
                                                    "|" +                                                                           //29 recv_xdock_facility_code
                                                    "|" +                                                                           //30 cust_field_1
                                                    "|" +                                                                           //31 cust_field_2
                                                    "|";                                                                            //32 cust_field_3

                                str.Append(cad_envio);
                                str.Append("\r\n");
                            }
                        }

                    }// End For

                    /*  Generando Archivos de Texto dentro de la carpeta WORD (localmente) */
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
                LogUtil.Graba_Log(Interface, ex.Message, true, "Procesa_Data_Det");
            }
            return dt_tabla;
        }

        private DataTable Procesa_Data_Cab(String modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompracx = null;

            string wCD = "";
            StringBuilder str = new StringBuilder();


            if (modo == "C")            // Es Chorillos
            {
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
            {
                wCD = "50003";
            }

            try
            {
                string sql_tocompracx = "EXEC [USP_ASN_CABECERA] " + wCD;

                //-----------------------
                //------ CABECERA -------
                //-----------------------

                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    /*selecccionando el archivo TOCOMPRACX */
                    using (SqlCommand cmd = new SqlCommand(sql_tocompracx, cn))
                    {
                        cmd.CommandTimeout = 5 * 60; // 5 minutos
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
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

                    dsc = new DataSet();
                    dsc.Tables.Add(dt_tocompracx);

                    string delimited = "|";
                    var strCab = new StringBuilder();

                    for (int ix = 0; ix < dsc.Tables[0].Rows.Count; ix++)
                    {
                        for (int j = 0; j < dsc.Tables[0].Columns.Count - 1; j++)
                        {
                            strCab.Append(dsc.Tables[0].Rows[ix][j].ToString() + delimited);    // Numero de orden de despacho
                        }
                        strCab.Append("\r\n");
                    }


                    /*  Generando Archivos de Texto */
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
    }// Asn_Purchase
}// CapaInterface