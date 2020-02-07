using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace CapaDatos
{
    public class ValidaCaracteres
    {
        public static string Numeros_Letras(string strIn)
        {
            strIn = strIn.ToString().Trim().Replace("Ñ", "N");
            strIn = strIn.ToString().Trim().Replace("ñ", "n");
            strIn = strIn.ToString().Trim().Replace("&", "y");
            strIn = strIn.ToString().Trim().Replace("@", "");
            strIn = strIn.ToString().Trim().Replace("¾", "");

            strIn = strIn.ToString().Trim().Replace("á", "a");
            strIn = strIn.ToString().Trim().Replace("é", "e");
            strIn = strIn.ToString().Trim().Replace("í", "i");
            strIn = strIn.ToString().Trim().Replace("ó", "o");
            strIn = strIn.ToString().Trim().Replace("ú", "u");

            strIn = strIn.ToString().Trim().Replace("Á", "A");
            strIn = strIn.ToString().Trim().Replace("É", "E");
            strIn = strIn.ToString().Trim().Replace("Í", "I");
            strIn = strIn.ToString().Trim().Replace("Ó", "O");
            strIn = strIn.ToString().Trim().Replace("Ú", "U");

            bool resultado = Regex.IsMatch(strIn, "^[a-zA-Z1-9]"); // valida el ingreso solo de Numeros y letras
            if (!resultado)
            {
                return "";
            }
            else
            {
                return strIn;
            }

        }

        public static string Email(string email)
        {
            email = email.ToString().Trim().Replace("Ñ", "N");
            email = email.ToString().Trim().Replace("ñ", "N");

            try
            {
                if (Regex.Replace(email, "\\w+([-+.']\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*", "").Length == 0)
                {
                    return email;
                }
                else
                {
                    return "";
                }

            }

            catch (Exception ex)
            {
                return ex.Message.ToString();
            }

        }



    }
}
