using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using WinSCP;

namespace CapaInterface
{
    public static class FTPUtil
    {
        public static bool Send_FTP_WMS(string file_origen, string file_destino, string wcd)
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

                    transferResult = session.PutFiles(file_origen, "/data/730/" + wcd + "/input/" + Path.GetFileName(file_destino), false, transferOptions);

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
                //LogUtil.Graba_Log("INTERFACE WMS", "ERROR AL SUBIR FTP: " + ex.Message,true,"");
                throw ex;
            }


            return exito;
        }
    }
}
