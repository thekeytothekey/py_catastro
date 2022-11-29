
# ----------------------------------------------------------------------------------------------------------
# ------------------------------------------------LIBRERIAS-------------------------------------------------
# ----------------------------------------------------------------------------------------------------------

import os
from pathlib import Path
from itertools import chain

import pandas as pd
import geopandas as gpd

import pandas_bokeh
pandas_bokeh.output_notebook()

# ----------------------------------------------------------------------------------------------------------

import requests
ses = requests.Session()
ses.trust_env = False
from ast import literal_eval
from urllib.parse import urlencode

# ----------------------------------------------------------------------------------------------------------

from nltk.corpus import stopwords
swes = set(stopwords.words("spanish"))

# ----------------------------------------------------------------------------------------------------------

from xml.etree.ElementTree import fromstring
from collections import OrderedDict
from xmljson import BadgerFish
bf = BadgerFish(dict_type=OrderedDict)
from json import dumps
import textdistance

# ----------------------------------------------------------------------------------------------------------

from shapely.geometry import Point, Polygon

# ----------------------------------------------------------------------------------------------------------
# ------------------------------------------------FUNCIONES-------------------------------------------------
# ---------------------------------Obtenidas de la librería utils_fc00232-----------------------------------
# ----------------------------------------------------------------------------------------------------------

def flatten_list(lista):
    '''
    Aplana una lista que dentro tiene elementos iterables
    '''
    return list(chain.from_iterable(lista))

# ----------------------------------------------------------------------------------------------------------

def flatten_dic(dic, limpia=lambda x: x, parent="", sep="__"): 
    '''
    Aplana un diccionario admitiendo una función 'limpia' en caso de que hubiera que preprocesar las keys (e.g. lambda x: x.string.split("}")`[-1])
    Para ver como funciona usar 'flatten_dic(prueba)'', donde 'prueba' es:
    
    prueba = {"a":{"1":{"1":"a__1__1",
                        "2":"a__1__2",
                        "3":{"1":"a__1__3__1",
                             "2":"a__1__3__2"}},
                   "2":{"1":"a__2__1"},
                   "3":"a__3"},
              "b":["b__e0",
                   "b__e1",
                   "b__e2"],
            "c":"c",
            "d":["d__e0",
                 "d__e1",
                {"1":"d__e2__1",
                 "2":"d__e2__2",
                 "3":["d__e2__3__e0",
                      {"1":"d__e2__3__e1__1",
                       "2":"d__e2__3__e1__2",
                       "3":["d__e2__3__e1__3__e0",
                            "d__e2__3__e1__3__e1"]},
                      "d__e2__3__e2"]}]}
    '''
    d = {}
    for k,v in dic.items():
        if type(v) == dict:
            d2 = flatten_dic(v,limpia,limpia(k))
            for k2,v2 in d2.items():
                if parent == "":
                    d[k2] = v2
                else:
                    d["{}{}{}".format(parent,sep,k2)] = v2
        elif type(v) == list:
            for i,e in enumerate(v):
                if type(e) == dict:
                    d2 = flatten_dic(e,limpia,"{}{}e{}".format(limpia(k),sep,i))
                    for k2,v2 in d2.items():
                        if parent == "":
                            d[k2] = v2
                        else:
                            d["{}{}{}".format(parent,sep,k2)] = v2
                else:
                    if parent=="":
                        d["{}{}e{}".format(limpia(k),sep,i)] = e
                    else:
                        d["{}{}{}{}e{}".format(parent,sep,limpia(k),sep,i)] = e
        else:
            if parent=="":
                d[limpia(k)] = v
            else:
                d["{}{}{}".format(parent,sep,limpia(k))] = v
    #print("Output: {}".format(d))
    return d

# ----------------------------------------------------------------------------------------------------------

def limpia_xml2json(dic, limpia=lambda x: x, marca="$"):
    '''
    Limpia un JSON fruto del parseo de un XML, admitiendo una función 'limpia' en caso de que hubiera que preprocesar las keys (e.g. lambda x: x.string.split("}")`[-1])
    Se espera un JSON generado mediante ast.literal_eval(json.dumps(xmljson.BadgerFish.data(xml.etree.ElementTree.fromstring(requests.get(<api_url>).content))))
    '''  
    d={}
    for k,v in dic.items():
        if type(v) == dict:
            try:
                if marca in v.keys():
                    d[limpia(k)] = v[marca]
                else:
                    d[limpia(k)] = limpia_xml2json(v,limpia)
            except:
                d[limpia(k)] = v
        elif type(v) == list:
            d[limpia(k)] = []
            for e in v:
                if type(e) == dict:
                    d[limpia(k)].append(limpia_xml2json(e,limpia))
                else:
                    d[limpia(k)].append(e)
        else:
            d[limpia(k)] = v
    return d

# ----------------------------------------------------------------------------------------------------------

def explode_pandas(df, cols, sep="__"):
    '''
    Convierte un dataframe en el que las columnas 'cols' tienen diccionarios en un dataframe con más columnas en las que cada una tiene datos y no diccionarios
    '''
    for col in cols:
        df2 = df[col].apply(pd.Series)
        df = pd.concat([df.drop([col],axis=1), df2.rename(columns={x:"{}{}{}".format(col,sep,x) for x in df2.columns})], axis=1)
    return df

# ----------------------------------------------------------------------------------------------------------
# --------------------------------------------------CLASE---------------------------------------------------
# -------------------------------------------Desarrollada adhoc---------------------------------------------
# ----------------------------------------------------------------------------------------------------------

class catastro_sesion:
    '''
    Objeto que guardará los datos consultados del Catastro usando su API
    '''

    def __init__(self, geodata_path=None):
        '''
        Creación de sesión para consulta de la API
        '''
        self.s = requests.Session()
        self.s.trust_env = False # Importante para que python pueda no tener que autenticarse contra el proxy
        self.ine_path = Path(os.path.dirname(__file__)).joinpath("GEOdata","cartografia_censo2011_nacional/SECC_CPV_E_20111101_01_R_INE.shp")
        self.crs = {'init': 'epsg:4326'} # Sistema de coordenadas para pintar datos en mapas
        self.sist_coords = pd.DataFrame({"SRS":["EPSG:4230","EPSG:4230","EPSG:4258","EPSG:32627","EPSG:32628","EPSG:32629","EPSG:32630",
                                                "EPSG:32631","EPSG:25829","EPSG:25830","EPSG:25831","EPSG:23029","EPSG:23030","EPSG:23031"],
                                         "Desc":["Geográficas en ED 50","Geográficas en WGS 80","Geográficas en ETRS89","UTM huso 27N en WGS 84","UTM huso 28N en WGS 84",
                                                 "UTM huso 29N en WGS 84","UTM huso 30N en WGS 84","UTM huso 31N en WGS 84","UTM huso 29N en ETRS89","UTM huso 30N en ETRS89",
                                                 "UTM huso 31N en ETRS89","UTM huso 29N en ED50","UTM huso 30N en ED50","UTM huso 31N en ED50"]})
        if not geodata_path:
            # Por defecto se carga el fichero del censo INE 2011
            try:
                # CUSEC es el código único de identificación de la sección censal, formado por: [CPRO(2)][CMUN(3)][CDIS(2)][CSEC(3)]
                self.secs = gpd.read_file(self.ine_path,encoding="latin")[["CUSEC","NCA","NPRO","NMUN","geometry"]]
                self.secs = self.secs.to_crs(self.crs)
            except:
                print("---> El fichero con los datos geográficos de las secciones censales del INE no se encuentra en la ruta por defecto:")
                print("---> {}".format(self.ine_path))
                print("---> Por favor especifique otra ruta si quiere representar en mapas los datos obtenidos o mapear fincas catastrales a secciones censales")
                print("---> llamando a la clase de la siguiente forma: query_catastro(<path>)")
                print("---> ¡NO SERÁ POSIBLE LA OBTENCIÓN DE SECCIONES CENSALES SI DECIDE CONTINUAR!")
                self.secs = None
        else:
            self.ine_path = geodata_path
            try:
                self.secs = gpd.read_file(geodata_path,encoding="latin")[["CUSEC","NCA","NPRO","NMUN","geometry"]]
                self.secs = self.secs.to_crs(self.crs)
            except KeyError:
                print("---> El fichero con los datos geográficos de las secciones censales del INE encotnrado en la ruta especificada no tiene el esquema esperado:")
                print("--->    + CUSEC (int) = Código único de identificación de la sección censal, formado por 10 dígitos")
                print("--->    + NCA (string) = Nombre de la comunidad autónoma de la sección censal")
                print("--->    + NPRO (string) = Nombre de la provincia de la sección censal")
                print("--->    + NMUN (string) = Nombre del municipio de la sección censal")
                print("--->    + geometry (shapely.geometry.Polygon) = Objecto con la información geográfica de la sección censal")
                print("---> Por favor especifique un fichero con un esquema válido para continuar")
                print("---> llamando a la clase de la siguiente forma: query_catastro(<path>)")
                print("---> ¡NO SERÁ POSIBLE LA OBTENCIÓN DE SECCIONES CENSALES SI DECIDE CONTINUAR!")
                self.secs = None
            except:
                print("---> El fichero con los datos geográficos de las secciones censales del INE no se encuentra en la ruta especificada:")
                print("---> {}".format(geodata_path))
                print("---> Por favor especifique otra ruta si quiere representar en mapas los datos obtenidos o mapear fincas catastrales a secciones censales")
                print("---> llamando a la clase de la siguiente forma: query_catastro(<path>)")
                print("---> ¡NO SERÁ POSIBLE LA OBTENCIÓN DE SECCIONES CENSALES SI DECIDE CONTINUAR!")
                self.secs = None

    def api_params(self):
        '''
        Genera un diccionario con las funciones y los parámetros correspondientes
        Recordar que la RC puede tener un número variable de digitos: 
            - 14 --> finca, devuelve todos los inmuebles en ésta
            - 18 ó 20 --> datos de un inmueble)
        '''
        params = {"provs":{"Obligatorios":[],"Opcionales":[]},
                  "munis_prov":{"Obligatorios":["Provincia","Municipio"],"Opcionales":[]},
                  "munis_prov_cods":{"Obligatorios":["CodigoProvincia"],"Opcionales":["CodigoMunicipio","CodigoMunicipioIne"]},
                  "dirs_muni":{"Obligatorios":["Provincia","Municipio","TipoVia","NombreVia"],"Opcionales":[]},
                  "dirs_muni_cods":{"Obligatorios":["CodigoProvincia","CodigoMunicipio,CodigoMunicipioIne"],"Opcionales":["CodigoVia"]},
                  "RC_dir":{"Obligatorios":["Provincia","Municipio","TipoVia","NomVia","Numero"],"Opcionales":[]},
                  "RC_dir_cods":{"Obligatorios":["CodigoProvincia","CodigoMunicipio,CodigoMunicipioIne","CodigoVia","Numero"],"Opcionales":[]},
                  "inmu_data":{"Obligatorios":["Provincia","Municipio","Calle","Numero"],"Opcionales":["Sigla","Bloque","Escalera","Planta","Puerta"]},
                  "inmu_data_cods":{"Obligatorios":["CodigoProvincia","CodigoMunicipio,CodigoMunicipioIne","CodigoVia","Numero"],
                                    "Opcionales":["Bloque","Escalera","Planta","Puerta"]},
                  "catdata_dir1":{"Obligatorios":["Provincia","Municipio","RC"],"Opcionales":[]},
                  "catdata_dir1_cods":{"Obligatorios":["RC"],"Opcionales":["CodigoProvincia","CodigoMunicipio,CodigoMunicipioIne"]},
                  "catdata_dir2":{"Obligatorios":["Provincia","Municipio","Poligono","Parcela"],"Opcionales":[]},
                  "catdata_dir2_cods":{"Obligatorios":["CodigoProvincia","CodigoMunicipio","CodigoMunicipioIne","Poligono","Parcela"],"Opcionales":[]},
                  "RC_coord_exact":{"Obligatorios":["SRS","Coordenada_X","Coordenada_Y"],"Opcionales":[]},
                  "RC_coord_aprox":{"Obligatorios":["SRS","Coordenada_X","Coordenada_Y"],"Opcionales":[]},
                  "loc_RC":{"Obligatorios":["Provincia","Municipio","RC","SRS"],"Opcionales":[]}}
        return(params)
        
    def api_url(self, funcion="", params={},**kwargs):
        '''
        Genera URLs de consulta para la API XML del Catastro
        '''
        urls = {"provs":"OVCCallejero.asmx/ConsultaProvincia",
                "munis_prov":"OVCCallejero.asmx/ConsultaMunicipio",
                "munis_prov_cods":"OVCCallejeroCodigos.asmx/ConsultaMunicipioCodigos",
                "dirs_muni":"OVCCallejero.asmx/ConsultaVia",
                "dirs_muni_cods":"OVCCallejeroCodigos.asmx/ConsultaViaCodigos",
                "RC_dir":"OVCCallejero.asmx/ConsultaNumero",
                "RC_dir_cods":"OVCCallejeroCodigos.asmx/ConsultaNumeroCodigos",
                "inmu_data":"OVCCallejero.asmx/Consulta_DNPLOC",
                "inmu_data_cods":"OVCCallejeroCodigos.asmx/Consulta_DNPLOC_Codigos",
                "catdata_dir1":"OVCCallejero.asmx/Consulta_DNPRC",
                "catdata_dir1_cods":"OVCCallejeroCodigos.asmx/Consulta_DNPRC_Codigos",
                "catdata_dir2":"OVCCallejero.asmx/Consulta_DNPPP",
                "catdata_dir2_cods":"OVCCallejeroCodigos.asmx/Consulta_DNPPP_Codigos",
                "RC_coord_exact":"OVCCoordenadas.asmx/Consulta_RCCOOR",
                "RC_coord_aprox":"OVCCoordenadas.asmx/Consulta_RCCOOR_Distancia",
                "loc_RC":"OVCCoordenadas.asmx/Consulta_CPMRC"}
        if funcion=="":
            return urls
        else:
            if params=={}:
                return "http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/{}".format(urls[funcion])
            else:
                return "http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/{}?{}".format(urls[funcion], urlencode({k:v.upper() for k,v in params.items()}))
    
    def query(self, url, verbose=False, flat=False,**kwargs):
        '''
        Consulta la API catastral haciendo un GET a la 'url' proporcionada, devolviendo el resultado en un JSON/diccionario con formato amigable.
        'verbose' ---> Si se quiere imprimir la URL consultada en las distintas operaciones para debugging
        'flat' ---> Si en lugar de un JSON con estructura se quiere un diccionario plano
        '''
        if verbose: print("URL: {}".format(url))
        if flat:
            return {k[:-3]:v for k,v in flatten_dic(literal_eval(dumps(bf.data(fromstring(self.s.get(url).content)))), lambda x: x.split("}")[-1]).items()}
        else:
            return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(self.s.get(url).content)))), lambda x: x.split("}")[-1])
    
    def provs(self,verbose=False):
        '''
        Obtiene un listado de las provincias existentes en el catastro
        '''
        if verbose: print("Consultando: {}".format(self.api_url("provs")))
        return pd.DataFrame(self.query(self.api_url("provs"))["consulta_provinciero"]["provinciero"]["prov"])

    def munis(self,prov,verbose=False,**kwargs):
        '''
        Obtiene un listado de todos los municipios dentro de una provincia 'prov' dada
        '''
        if verbose: print("Consultando: {}".format(self.api_url("munis_prov",{"Provincia":prov,"Municipio":""})))
        return pd.DataFrame([flatten_dic(x) for x in self.query(self.api_url("munis_prov",{"Provincia":prov,"Municipio":""}))["consulta_municipiero"]["municipiero"]["muni"]])

    def muni_in_prov(self, prov, muni, verbose=False,**kwargs):
        '''
        Comprueba si el municipio 'muni' aportado se encuentra en la provincia 'prov' aportada
        Si no se encuentra pero hay una alta similitud con otro, se sugiere el candidato
        '''

        def quita_tokens(string):
            return [token.upper() for token in string.split() if token.lower() not in (swes | set([string.lower()]))]

        correciones = {"STA":"SANTA","STO":"SANTO","STA.":"SANTA","STO.":"SANTOS"}
        eps = 1e-3
        for k,v in correciones.items():
            muni = muni.upper().replace(k,v)
        muni = muni.replace(" ","")
        munis = self.munis(prov,verbose)
        r = list(munis[munis["nm"].str.contains(muni)]["nm"])
        if len(munis[munis["nm"] == muni])>0:
            return {"Resultado":True,"Sugerencias":None}
        elif len(r)>0:
            if verbose: print("Usando coincidencia parcial encontrada")
            return {"Resultado":False,"Sugerencias":r}
        else:
            #tokens = [token.upper() for token in muni.split() if token.lower() not in (swes | set([muni.lower()]))]
            tokens = quita_tokens(muni)
            r = []
            for token in tokens:
                r.append(list(munis[munis["nm"].str.contains(token)]["nm"]))
            r = flatten_list(r)
            if len(r) > 0:
                if verbose: print("Usando tokens...")
                munis = munis[munis["nm"].isin(set(r))]
            munis["lev"] = munis["nm"].apply(lambda x: textdistance.levenshtein.distance("".join(quita_tokens(x)),muni))
            munis["prop"] = munis["nm"].apply(lambda x: len(set(x) & set(muni))*abs(len("".join(quita_tokens(x)))-len(muni)))
            #r_lev = (munis[munis["lev"] == munis["lev"].min()]["nm"])
            #r_prop = list(munis[munis["prop"] == munis["prop"].max()]["nm"])
            r_lev = munis.loc[munis["lev"].idxmin()]["nm"]
            r_prop = munis.loc[munis["prop"].idxmax()]["nm"]
            s = pd.Series({x:abs(len("".join(quita_tokens(x)))/len(muni)-1) for x in flatten_list([[r_lev,r_prop]])})
            if verbose: print(s)
            if s.max() < 1 and muni.count(" ") != r_lev.count(" "):
                if verbose: print("Usando distancia basada en Jaccard")
                return {"Resultado":False, "Sugerencias":r_prop}
            else:
                if verbose: print("Usando distancia basada en Levenshtein")
                return {"Resultado":False, "Sugerencias":r_lev}
    
    def callejero(self, prov,muni,verbose=False,**kwargs):
        '''
        Obtiene el callejero (conjunto de vías) del municipio aportado 'muni' en la provincia 'prov'
        Si no encuentra el municipio, utiliza la función 'muni_in_prov' de la clase para encontrar parecidos/sugerencias
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("dirs_muni"),prov,muni,"",""]]
        url = "{}?Provincia={}&Municipio={}&TipoVia={}&NombreVia={}".format(*d)
        if verbose: print("Consultando: {}".format(url))
        try:
            return explode_pandas(pd.DataFrame(limpia_xml2json(self.query(url),lambda x: x)["consulta_callejero"]["callejero"]["calle"]),["loine","dir"])
        except:
            return ("ERROR","Municipio no existe o no se puede encontrar con suficiente seguridad",[])

    def rc(self, prov,muni,tv,nv,num,verbose=False,**kwargs):
        '''
        TODO
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("RC_dir"),prov,muni,tv,nv,num]]
        url = "{}?Provincia={}&Municipio={}&TipoVia={}&NomVia={}&Numero={}".format(*d)
        if verbose: print("Consultando: {}".format(url))
        return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])

    def rc_cod(self, cod_prov,cod_muni,cod_v,num,verbose=False,**kwargs):
        '''
        TODO
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("RC_dir_cods"),cod_prov,cod_muni,cod_muni,cod_v,num]]
        url = "{}?CodigoProvincia={}&CodigoMunicipio={}&CodigoMunicipioIne={}&CodigoVia={}&Numero={}".format(*d)
        if verbose: print("Consultando: {}".format(url))
        return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])    

    def hay_RC(self, x,num,verbose=False,**kwargs):
        '''
        TODO
        '''
        r = self.rc_cod(x.loine__cp,x.loine__cm,x.dir__cv,num,verbose)["consulta_numerero"]
        if verbose: print(r)
        if "cunum" in r["control"]:
            if r["control"]["cunum"] == 1:
                return ("ERROR","Direccion similar encontrada",
                        [("{}---{}---{}".format(x.dir__tv,x.dir__nv,num),
                          "{}{}".format(r["numerero"]["nump"]["pc"]["pc1"],r["numerero"]["nump"]["pc"]["pc2"]))])
            else:
                if verbose: print(r["numerero"]["nump"])
                return ("ERROR","Número incorrecto, se dan aproximaciones",
                        [("{}---{}---{}".format(x.dir__tv,x.dir__nv,y["num"]["pnp"]),
                          "{}{}".format(y["pc"]["pc1"],y["pc"]["pc2"])) for y in r["numerero"]["nump"]])
        else:
            return ("ERROR","Dirección no encontrada",[])

    def RC(self, prov,muni,tv,nv,num,verbose=False,intense=True,**kwargs):
        '''
        Devuelve el sgte esquema: (OK/ERROR,"Descripción del error",lista de tuplas (cp,cpro,cmun,cv,num,direccion,RC))
        '''
        if verbose: print("Intentando obtener info con los datos proporcionados")
        r = self.rc(prov,muni,tv,nv,num,verbose)["consulta_numerero"]
        if verbose: print("Resultado: {}".format(r))
        if "cuerr" in r["control"]:
            if intense:
                if "numerero" in r:
                    nums = [x["num"]["pnp"] for x in r["numerero"]["nump"]]
                    if verbose: print("Número incorrecto, se dan aproximaciones: {}".format(nums))
                    return("ERROR","Número incorrecto, se dan aproximaciones",
                           [("{}---{}---{}".format(tv.upper(),nv.upper(),y["num"]["pnp"]),
                             "{}{}".format(y["pc"]["pc1"],y["pc"]["pc2"])) for y in r["numerero"]["nump"]])
                else:
                    if r['lerr']['err']['cod'] == 22: 
                        # pueblo no existe
                        if verbose: print("Municipio inválido, buscando municipios en la provincia {}".format(prov))
                        munis = self.muni_in_prov(prov,muni,verbose)
                        if verbose: print("Resultado: {}".format(munis))
                        if len(munis["Sugerencias"])==1:
                            print("---> Corrigiendo municipio {} por {} en la provincia de {}".format(muni,munis["Sugerencias"][0],prov))
                            muni = munis["Sugerencias"][0]
                            return RC(prov,muni,tv,nv,num,verbose)
                        else:
                            return ("ERROR","Municipio no existe o no se puede encontrar con suficiente seguridad",[])
                    elif r['lerr']['err']['cod'] == 33:
                        # vía no existe
                        result = []
                        tokens = [token.upper() for token in nv.split() if token.lower() not in (swes | set([muni.lower()]))]
                        dirs = self.callejero(prov,muni,verbose)
                        dirs2 = dirs[dirs["dir__nv"].astype(str) == nv.upper()]
                        if len(dirs2) != 0:
                            dirs2["RC"] = dirs2.apply(lambda x: self.hay_RC(x,num,verbose), axis=1)
                            result = [x for x in list(dirs2["RC"]) if x[2]!=[]]
                            return ("ERROR","Dirección no encontrada",result)
                        else:
                            if type(dirs) != tuple:
                                for token in tokens:
                                    if verbose: print("Dirección inválida, buscando en el callejero del municipio proporcionado el término {}".format(token))
                                    dirs2 = dirs[dirs["dir__nv"].astype(str).str.contains(token)]
                                    if dirs2.shape[0] != 0:
                                        dirs2["RC"] = dirs2.apply(lambda x: self.hay_RC(x,num,verbose), axis=1)
                                        result.append([x for x in list(dirs2["RC"]) if x[2]!=[]])
                                if len(result)==0:
                                    return ("ERROR","Dirección no encontrada",result)
                                else:
                                    return ("ERROR","Direcciones aproximadas",result)
                            else:
                                return dirs
                    elif r['lerr']['err']['cod'] == 43:
                        # número no existe en la vía según catastro
                        return ("ERROR","Dirección no encontrada",[])
            else:
                # búsqueda cortada para que no sea muy intenso en recursos
                return ("ERROR","Dirección no encontrada",[])  
        else:
            if type(r["numerero"]["nump"]) == list:
                return ("OK","",[("{}---{}---{}".format(tv.upper(),nv.upper(),num),"{}{}".format(*x["pc"].values())) for x in r["numerero"]["nump"]])
            else:
                return ("OK","",[("{}---{}---{}".format(tv.upper(),nv.upper(),num),"{}{}".format(*r["numerero"]["nump"]["pc"].values()))])


    def datos_rc(self, prov,muni,rc,verbose=False,**kwargs):
        '''
        TODO
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("catdata_dir1"),prov,muni,rc]]
        url = "{}?Provincia={}&Municipio={}&RC={}".format(*d)
        if verbose: print(url)
        return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])

    def datos_loc(self, prov,muni,nv,num,sigla,bloque,escalera,planta,puerta,verbose=False,**kwargs):
        '''
        TODO
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("inmu_data"),prov,muni,nv,num,sigla,bloque,escalera,planta,puerta]]
        url = "{}?Provincia={}&Municipio={}&Calle={}&Numero={}&Sigla={}&Bloque={}&Escalera={}&Planta={}&Puerta={}".format(*d)
        if verbose: print(url)
        return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])

    def datos_loc_cods(self, cod_prov,cod_muni,cod_v,num,bloque="",escalera="",planta="",puerta="",verbose=False,**kwargs):
        '''
        TODO
        '''
        d = [str(x).replace(" ","+") for x in [self.api_url("inmu_data_cods"),cod_prov,cod_muni,cod_muni,cod_v,num,bloque,escalera,planta,puerta]]
        url = "{}?CodigoProvincia={}&CodigoMunicipio={}&CodigoMunicipioIne={}&CodigoVia={}&Numero={}&Bloque={}&Escalera={}&Planta={}&Puerta={}".format(*d)
        if verbose: print(url)
        return limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])

    def corrige(self,n,d):
        '''
        TODO
        '''
        return "0000000{}".format(n)[-d:]

    def escanea_rc(self, prov,muni,rc,verbose=False,**kwargs):
        '''
        TODO
        '''
    #     try:
        if len(rc) < 14: rc = self.corrige(rc,14)
        r = self.datos_rc(prov,muni,rc,verbose)["consulta_dnp"]
        if verbose: print(r)
        res = []
        if r["control"]["cudnp"] == 1:
            d = {}
            d["rc"] = rc
            d["cod_prov_rc"] = r["bico"]["bi"]["dt"]["loine"]["cp"]
            d["cod_mun_rc"] = r["bico"]["bi"]["dt"]["loine"]["cm"]
            if "lous" in r["bico"]["bi"]["dt"]["locs"]:
                d["cod_v_rc"] = r["bico"]["bi"]["dt"]["locs"]["lous"]["lourb"]["dir"]["cv"]
            elif "lors" in r["bico"]["bi"]["dt"]["locs"]:
                d["cod_v_rc"] = r["bico"]["bi"]["dt"]["locs"]["lors"]["lourb"]["dir"]["cv"]
            d["dir_rc"] = r["bico"]["bi"]["ldt"]
            if "luso" in r["bico"]["bi"]["debi"]:
                d["info_uso_rc"] = r["bico"]["bi"]["debi"]["luso"]
            else:
                d["info_us_rc"] = ""
            d["info_tam_rc"] = r["bico"]["bi"]["debi"]["sfc"]
            if "cpt" in r["bico"]["bi"]["debi"]:
                d["info_cpt_rc"] = r["bico"]["bi"]["debi"]["cpt"]
            else:
                d["info_cpt_rc"] = ""
            if "ant" in r["bico"]["bi"]["debi"]:
                d["info_ant_rc"] = r["bico"]["bi"]["debi"]["ant"]
            else:
                d["info_ant_rc"] = ""
            if type(r["bico"]["lcons"]["cons"]) == list:
                d["info_sub_rc"] = [(x["lcd"],x["dfcons"]["stl"]) for x in r["bico"]["lcons"]["cons"]]
            else:
                d["info_sub_rc"] = (r["bico"]["lcons"]["cons"]["lcd"],r["bico"]["lcons"]["cons"]["dfcons"]["stl"])
            if verbose: print(d)
            res.append(d)
        else:
            rcs = ["{}{}{}{}{}".format(self.corrige(x["rc"]["pc1"],7),x["rc"]["pc2"],self.corrige(x["rc"]["car"],4),x["rc"]["cc1"],x["rc"]["cc2"]) for x in r["lrcdnp"]["rcdnp"]]
            for rci in rcs:
                r2 = self.datos_rc(prov,muni,rci,verbose)["consulta_dnp"]
                if verbose: print(r2)
                d = {}
                d["rc"] = rci
                d["cod_prov_rc"] = r2["bico"]["bi"]["dt"]["loine"]["cp"]
                d["cod_mun_rc"] = r2["bico"]["bi"]["dt"]["loine"]["cm"]
                if "lous" in r2["bico"]["bi"]["dt"]["locs"]:
                    d["cod_v_rc"] = r2["bico"]["bi"]["dt"]["locs"]["lous"]["lourb"]["dir"]["cv"]
                elif "lors" in r2["bico"]["bi"]["dt"]["locs"]:
                    d["cod_v_rc"] = r2["bico"]["bi"]["dt"]["locs"]["lors"]["lourb"]["dir"]["cv"]
                d["dir_rc"] = r2["bico"]["bi"]["ldt"]
                if "luso" in r2["bico"]["bi"]["debi"]:
                    d["info_uso_rc"] = r2["bico"]["bi"]["debi"]["luso"]
                else:
                    d["info_us_rc"] = ""
                d["info_tam_rc"] = r2["bico"]["bi"]["debi"]["sfc"]
                if "cpt" in r2["bico"]["bi"]["debi"]:
                    d["info_cpt_rc"] = r2["bico"]["bi"]["debi"]["cpt"]
                else:
                    d["info_cpt_rc"] = ""
                if "ant" in r2["bico"]["bi"]["debi"]:
                    d["info_ant_rc"] = r2["bico"]["bi"]["debi"]["ant"]
                else:
                    d["info_ant_rc"] = ""
                if type(r2["bico"]["lcons"]["cons"]) == list:
                    d["info_sub_rc"] = [(x["lcd"],x["dfcons"]["stl"]) for x in r2["bico"]["lcons"]["cons"]]
                else:
                    d["info_sub_rc"] = (r2["bico"]["lcons"]["cons"]["lcd"],r2["bico"]["lcons"]["cons"]["dfcons"]["stl"])
                if verbose: print(d)
                res.append(d)
        return res
    #     except Exception as e:
    #         print(e)
    #         return "ERROR"

    def map_rc2coord(self,rc, prov="",muni="",verbose=False,srs="EPSG:4326",**kwargs):
        '''
        TODO
        '''
        prov = prov.upper()
        muni = muni.upper()
        if len(rc) < 14: rc = self.corrige(rc,14)
        d = [srs,prov.replace(" ","+"),muni.replace(" ","+"),rc[:14]]
        url = "{}?SRS={}&Provincia={}&Municipio={}&RC={}".format(self.api_url("loc_RC"),*d)
        if verbose: print("Consultando: {}".format(url))
        j = limpia_xml2json(literal_eval(dumps(bf.data(fromstring(ses.get(url).content)))),lambda x: x.split("}")[-1])["consulta_coordenadas"]["coordenadas"]["coord"]
        if verbose: print("Resultado: {}".format(j))
        if prov!="" and muni!="":
            return {"long":j["geo"]["xcen"],"lat":j["geo"]["ycen"],"prov":prov,"muni":muni,"norm_dir":j["ldt"],"rc":rc}, [Point(j["geo"]["xcen"],j["geo"]["ycen"])]
        else:
            return {"long":j["geo"]["xcen"],"lat":j["geo"]["ycen"],"norm_dir":j["ldt"],"rc":rc}, [Point(j["geo"]["xcen"],j["geo"]["ycen"])]

    def pinta_rc(self,rc,plot=True,verbose=False,**kwargs):
        '''
        TODO
        '''
        r, geometry = self.map_rc2coord(rc,prov="",muni="",verbose=verbose)
        if verbose: print(r)

        geodf = gpd.GeoDataFrame(r,index=[0],crs=self.crs,geometry=geometry)
        if plot: geodf.plot_bokeh(hovertool_string="<div>DIR: {}</div><div>RC: {}</div>".format(geodf["norm_dir"][0],rc),legend="Finca",
                                  size=10,color="red",marker="o")

        return geodf

    def rc2sc(self,rc,plot=False,verbose=False,**kwargs):
        '''
        TODO
        '''
        try:
            geodf = self.pinta_rc(rc,verbose=verbose,plot=False)
            sc = self.secs.iloc[[self.secs["geometry"].apply(lambda x: geodf["geometry"][0].within(x)).idxmax()]]
            if plot:
                fig = sc.plot_bokeh(hovertool_string="<div>CodSC: {}</div><div>CCAA: {}</div><div>PROV: {}</div><div>MUN: {}</div>".format(*sc.reset_index().iloc[0].tolist()[1:-1]),legend="Sección censal",
                                    fill_alpha=0.3,show_figure=False)
                geodf.plot_bokeh(hovertool_string="<div>DIR: {}</div><div>RC: {}</div>".format(geodf["norm_dir"][0],geodf["rc"][0]),legend="Finca",
                                 figure=fig,size=10,color="red",marker="o")
            return sc
        except NameError:
            print("---> Ha habido un error en la carga de la información geográfica de las secciones censales")
            print("---> Por favor especifique otra ruta si quiere mapear fincas catastrales a secciones censales")
            print("---> llamando a la clase de la siguiente forma: query_catastro(<path>)")
            print("---> ¡NO SERÁ POSIBLE LA OBTENCIÓN DE SECCIONES CENSALES SI DECIDE CONTINUAR!")
            return 1