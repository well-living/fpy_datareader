# -*- coding: utf-8 -*-
"""
総務省統計局e-StatAPIからデータを取得する
https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0

author: WeLLiving@well-living
"""

import time
import urllib

import numpy as np
import pandas as pd
import requests

class etatReader():
    """
    
    """

    @property
    def url(self, version="3.0"):
        """API URL"""
        return "https://api.e-stat.go.jp/rest/%s/app/json/" % str(version)









    """
    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used.
    freq : {str, None}
        Frequency to use in select readers
    """


    _chunk_size = 1024 * 1024
    _format = "string"

    def __init__(
        self,
        symbols,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        freq=None,
    ):

        self.symbols = symbols

        start, end = _sanitize_dates(start or self.default_start_date, end)
        self.start = start
        self.end = end

        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout
        self.pause_multiplier = 1
        self.session = _init_session(session)
        self.freq = freq
        self.headers = None

    def close(self):
        """Close network session"""
        self.session.close()

    @property
    def default_start_date(self):
        """Default start date for reader. Defaults to 5 years before current date"""
        today = datetime.date.today()
        return today - datetime.timedelta(days=365 * 5)

    @property
    def url(self):
        """API URL"""
        # must be overridden in subclass
        raise NotImplementedError

    @property
    def params(self):
        """Parameters to use in API calls"""
        return None

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        if self._format == "string":
            out = self._read_url_as_StringIO(url, params=params)
        elif self._format == "json":
            out = self._get_response(url, params=params).json()
        else:
            raise NotImplementedError(self._format)
        return self._read_lines(out)

    def _read_url_as_StringIO(self, url, params=None):
        """
        Open url (and retry)
        """
        response = self._get_response(url, params=params)
        text = self._sanitize_response(response)
        out = StringIO()
        if len(text) == 0:
            service = self.__class__.__name__
            raise IOError(
                "{} request returned no data; check URL for invalid "
                "inputs: {}".format(service, self.url)
            )
        if isinstance(text, binary_type):
            out.write(bytes_to_str(text))
        else:
            out.write(text)
        out.seek(0)
        return out

    @staticmethod
    def _sanitize_response(response):
        """
        Hook to allow subclasses to clean up response data
        """
        return response.content

    def _get_response(self, url, params=None, headers=None):
        """send raw HTTP request to get requests.Response from the specified url
        Parameters
        ----------
        url : str
            target URL
        params : dict or None
            parameters passed to the URL
        """
        headers = headers or self.headers
        pause = self.pause
        last_response_text = ""
        for _ in range(self.retry_count + 1):
            response = self.session.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
            if response.status_code == requests.codes.ok:
                return response

            if response.encoding:
                last_response_text = response.text.encode(response.encoding)
            time.sleep(pause)

            # Increase time between subsequent requests, per subclass.
            pause *= self.pause_multiplier
            # Get a new breadcrumb if necessary, in case ours is invalidated
            if isinstance(params, list) and "crumb" in params:
                params["crumb"] = self._get_crumb(self.retry_count)

            # If our output error function returns True, exit the loop.
            if self._output_error(response):
                break

        if params is not None and len(params) > 0:
            url = url + "?" + urlencode(params)
        msg = "Unable to read URL: {0}".format(url)
        if last_response_text:
            msg += "\nResponse Text:\n{0}".format(last_response_text)

        raise RemoteDataError(msg)

    def _get_crumb(self, *args):
        """To be implemented by subclass"""
        raise NotImplementedError("Subclass has not implemented method.")

    def _output_error(self, out):
        """If necessary, a service can implement an interpreter for any non-200
         HTTP responses.
        Parameters
        ----------
        out: bytes
            The raw output from an HTTP request
        Returns
        -------
        boolean
        """
        return False

    def _read_lines(self, out):
        rs = read_csv(out, index_col=0, parse_dates=True, na_values=("-", "null"))[::-1]
        # Needed to remove blank space character in header names
        rs.columns = list(map(lambda x: x.strip(), rs.columns.values.tolist()))

        # Yahoo! Finance sometimes does this awesome thing where they
        # return 2 rows for the most recent business day
        if len(rs) > 2 and rs.index[-1] == rs.index[-2]:  # pragma: no cover
            rs = rs[:-1]
        # Get rid of unicode characters in index name.
        try:
            rs.index.name = rs.index.name.decode("unicode_escape").encode(
                "ascii", "ignore"
            )
        except AttributeError:
            # Python 3 string has no decode method.
            rs.index.name = rs.index.name.encode("ascii", "ignore").decode()

        return rs


    @property
    def url(self):
        """API URL"""
        return "https://api.e-stat.go.jp/rest/%s/app/json/" % str(version)

    def read(self):
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        names = self.symbols
        urls = ["{}?id={}".format(self.url, n) for n in names]

        def fetch_data(url, name):
            """Utillity to fetch data"""
            resp = self._read_url_as_StringIO(url)
            data = pd.read_csv(
                resp,
                index_col=0,
                parse_dates=True,
                header=None,
                skiprows=1,
                names=["DATE", name],
                na_values=".",
            )
            try:
                return data.truncate(self.start, self.end)
            except KeyError:  # pragma: no cover
                if data.iloc[3].name[7:12] == "Error":
                    raise IOError(
                        "Failed to get the data. Check that "
                        "{0!r} is a valid FRED series.".format(name)
                    )
                raise

        df = pd.concat(
            [fetch_data(url, n) for url, n in zip(urls, names)], axis=1, join="outer"
        )
        return df





#%%
def api_info(version='3.0'):
    """
    Parameters
    ----------
    version : string
        e-Stat APIのバージョン. The default is '3.0'.

    Returns
    -------
    request_urls : dict
        各e-Stat APIのURLを辞書型で返す.
    
    Reference
    -------
        https://www.e-stat.go.jp/api/api-info/e-stat-manual
    """
    
    url = 'https://api.e-stat.go.jp/rest/%s/app/json/' % str(version)
    request_urls = {}
    request_urls.update({'統計表情報取得': url + 'getStatsList?'})
    request_urls.update({'メタ情報取得': url + 'getMetaInfo?'})
    request_urls.update({'統計データ取得': url + 'getStatsData?'})
    request_urls.update({'データセット参照': url + 'refDataset?'})
    request_urls.update({'データカタログ情報取得': url + 'getDataCatalog?'})
    return request_urls

#%%
class eStatReader:
    def __init__(self, appId, version='3.0'):
        """
        Parameters
        ----------
        appId : string
            取得したアプリケーションIDを指定.
        version : string, float
            e-Stat APIのバージョン. The default is '3.0'.

        Returns
        -------
        None.

        """
        self.appId = appId
        self.version = version

#%%
    # e-Statのデータのリストを取得
    def get_StatsList(self, to_csv=False, path=''):
        """
        統計表情報取得(約180,000件)
        
        Parameters
        ----------
        to_csv : bool
            Trueの場合,CSVファイル出力. The default is False.
        path : string
            CSVファイル出力時のパス. The default is ''.
    
        Returns
        -------
        TABLE_INF : dict
            統計表の情報をNUMBERの件数分出力します。属性として統計表ID(id)を保持.
            ['GET_STATS_LIST']['DATALIST_INF']['TABLE_INF']
            
            TABLE_INF.columns
                ['@id', 'STAT_NAME', 'GOV_ORG', 'STATISTICS_NAME', 'TITLE',
                 'CYCLE', 'SURVEY_DATE', 'OPEN_DATE', 'SMALL_AREA', 'COLLECT_AREA',
                 'MAIN_CATEGORY', 'SUB_CATEGORY', 'OVERALL_TOTAL_NUMBER', 'UPDATED_DATE', 'STATISTICS_NAME_SPEC',
                 'DESCRIPTION', 'TITLE_SPEC']
                
        STATUS : int
            0～2の場合は正常終了、100以上の場合はエラー.
            ['GET_STATS_LIST']['RESULT']['STATUS'] 
            
        DATE : date
            このJSONデータが出力された日時.
            ['GET_STATS_LIST']['RESULT']['DATE'] 
            
        NUMBER : int
            出力される統計表の件数. 約180,000件
            ['GET_STATS_LIST']['DATALIST_INF']['NUMBER'] 
            
        RESULT_INF : int
            データの開始位置、データの終了位置.
            ['GET_STATS_LIST']['DATALIST_INF']['RESULT_INF']
        
        Reference
        -------
        出力しない項目
        ['GET_STATS_LIST']['RESULT']['ERROR_MSG'] : STATUSの値に対応するエラーメッセージ
        ['GET_STATS_LIST']['PARAMETER']['LANG'] : 言語
        ['GET_STATS_LIST']['PARAMETER']['DATA_FORMAT'] : 出力フォーマット形式「X」：XML形式「J」：JSON形式又はJSONP形式
        
        """
        request_url_str = 'https://api.e-stat.go.jp/rest/%s/app/json/getStatsList?' % str(self.version)
        params = {
            'appId': self.appId,
        }
        params_str = urllib.parse.urlencode(query=params)
        request_url_str += params_str
        jsn = requests.get(request_url_str).json()
        # 主要な統計表情報APIの出力データ取得
        STATUS = jsn['GET_STATS_LIST']['RESULT']['STATUS']
        DATE = jsn['GET_STATS_LIST']['RESULT']['DATE']
        
        NUMBER = jsn['GET_STATS_LIST']['DATALIST_INF']['NUMBER'] 
        RESULT_INF = jsn['GET_STATS_LIST']['DATALIST_INF']['RESULT_INF'] 
        
        TABLE_INF = pd.DataFrame(jsn['GET_STATS_LIST']['DATALIST_INF']['TABLE_INF'])
        
        # TABLE_INFの各セルのディクショナリ型データを結合可能なDataFrameに変換
        # STAT_NAME, GOV_ORG, TITLE, MAIN_CATEGORY, SUB_CATEGORY, STATISTICS_NAME_SPEC, TITLE_SPEC
        
        # STAT_NAME
        STAT_NAME = []
        for i, dct in enumerate(TABLE_INF['STAT_NAME']):
            df_tmp = pd.DataFrame(dct, index=[i])
            df_tmp.columns = ['STAT_NAME' + '_' + col for col in df_tmp.columns]
            STAT_NAME += [df_tmp]
        STAT_NAME = pd.concat(STAT_NAME, axis=0)
        # GOV_ORG
        GOV_ORG = []
        for i, dct in enumerate(TABLE_INF['GOV_ORG']):
            df_tmp = pd.DataFrame(dct, index=[i])
            df_tmp.columns = ['GOV_ORG' + '_' + col for col in df_tmp.columns]
            GOV_ORG += [df_tmp]
        GOV_ORG = pd.concat(GOV_ORG, axis=0)
        # TITLE
        TITLE = []
        for i, dct in enumerate(TABLE_INF['TITLE']):
            if type(dct) == dict:
                df_tmp = pd.DataFrame(dct, index=[i])
                df_tmp.columns = ['TITLE' + '_' + col for col in df_tmp.columns]
                TITLE += [df_tmp]
            elif type(dct) == str:
                df_tmp = pd.DataFrame([[0, dct]], index=[i])
                df_tmp.columns = ['TITLE_@no', 'TITLE_$']
            else:
                print(dct + "は結合できません。")
        TITLE = pd.concat(TITLE, axis=0)
        # MAIN_CATEGORY
        MAIN_CATEGORY = []
        for i, dct in enumerate(TABLE_INF['MAIN_CATEGORY']):
            df_tmp = pd.DataFrame(dct, index=[i])
            df_tmp.columns = ['MAIN_CATEGORY' + '_' + col for col in df_tmp.columns]
            MAIN_CATEGORY += [df_tmp]
        MAIN_CATEGORY = pd.concat(MAIN_CATEGORY, axis=0)
        # SUB_CATEGORY
        SUB_CATEGORY = []
        for i, dct in enumerate(TABLE_INF['SUB_CATEGORY']):
            df_tmp = pd.DataFrame(dct, index=[i])
            df_tmp.columns = ['SUB_CATEGORY' + '_' + col for col in df_tmp.columns]
            SUB_CATEGORY += [df_tmp]
        SUB_CATEGORY = pd.concat(SUB_CATEGORY, axis=0)
        # STATISTICS_NAME_SPEC
        STATISTICS_NAME_SPEC = []
        for i, dct in enumerate(TABLE_INF['STATISTICS_NAME_SPEC']):
            if type(dct) == dict:
                df_tmp = pd.DataFrame(dct, index=[i])
                df_tmp.columns = ['STATISTICS_NAME_SPEC' + '_' + col for col in df_tmp.columns]
                STATISTICS_NAME_SPEC += [df_tmp]
            else:
                print(dct + "は結合できません。")
        STATISTICS_NAME_SPEC = pd.concat(STATISTICS_NAME_SPEC, axis=0)
        # TITLE_SPEC
        TITLE_SPEC = []
        for i, dct in enumerate(TABLE_INF['TITLE_SPEC']):
            if type(dct) == dict:
                df_tmp = pd.DataFrame(dct, index=[i])
                df_tmp.columns = ['TITLE_SPEC' + '_' + col for col in df_tmp.columns]
                TITLE_SPEC += [df_tmp]
            else:
                print(dct + "は結合できません。")
        TITLE_SPEC = pd.concat(TITLE_SPEC, axis=0)
        # 
        TABLE_INF = pd.concat([TABLE_INF, STAT_NAME, GOV_ORG, TITLE, MAIN_CATEGORY, SUB_CATEGORY, STATISTICS_NAME_SPEC, TITLE_SPEC], axis=1)
        TABLE_INF = TABLE_INF.drop(['STAT_NAME', 'GOV_ORG', 'TITLE','MAIN_CATEGORY', 'SUB_CATEGORY', 'STATISTICS_NAME_SPEC', 'TITLE_SPEC'], axis=1)
        
        if to_csv:
            TABLE_INF.to_csv(path+'estat_statslist_table_inf.csv')
        
        return TABLE_INF, STATUS, DATE, NUMBER, RESULT_INF

#%%
    # e-Statのデータカタログ取得
    def get_estat_DataCatalog(self, limit=100):
        """
        データカタログ取得
        
        Parameters
        ----------
        appId : string
            取得したアプリケーションIDを指定.
        limit : int
            データセット取得件数
        version : string, float
            e-Stat APIのバージョン. The default is '3.0'.
    
        Returns
        -------
        DATASET : pandas.core.frame.DataFrame
            カタログデータセット情報を保持.
            ['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['DATA_CATALOG_INF']['DATASET']
        CATAROG_id : pandas.core.series.Series
            カタログデータセットIDを保持.
            ['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['DATA_CATALOG_INF']['@id']
        STATUS : int
            0～2の場合は正常終了、100以上の場合はエラー.
            ['GET_STATS_LIST']['RESULT']['STATUS'] 
            
        DATE : date
            このJSONデータが出力された日時.
            ['GET_STATS_LIST']['RESULT']['DATE'] 
            
        NUMBER : int
            出力される統計表の件数. 約180,000件
            ['GET_STATS_LIST']['DATALIST_INF']['NUMBER'] 
            
        RESULT_INF : int
            データの開始位置、データの終了位置.
            ['GET_STATS_LIST']['DATALIST_INF']['RESULT_INF']
    
        
        Reference
        -------
        出力しない項目
        ['GET_DATA_CATALOG']['RESULT']['ERROR_MSG'] : STATUSの値に対応するエラーメッセージです。
        ['GET_DATA_CATALOG']['PARAMETER']['LANG'] : 言語
        ['GET_DATA_CATALOG']['PARAMETER']['DATA_FORMAT'] : 出力フォーマット形式「X」：XML形式「J」：JSON形式又はJSONP形式
        ['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['DATA_CATALOG_INF'] : len 100
        """
        
        request_url_str = "https://api.e-stat.go.jp/rest/%s/app/json/getDataCatalog?" % str(self.version)
        params = {
            "appId": self.appId,
            "limit": limit
        }
        params_str = urllib.parse.urlencode(query=params)  # urllib
        request_url_str += params_str
        jsn = requests.get(request_url_str).json()
    
        STATUS = jsn['GET_DATA_CATALOG']['RESULT']['STATUS']
        DATE = jsn['GET_DATA_CATALOG']['RESULT']['DATE'] 
        NUMBER = jsn['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['NUMBER'] 
        RESULT_INF = jsn['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['RESULT_INF'] 
    
        DATA_CATALOG_INF = pd.DataFrame(jsn['GET_DATA_CATALOG']['DATA_CATALOG_LIST_INF']['DATA_CATALOG_INF'])
    
        CATAROG_id = DATA_CATALOG_INF['@id']
        
        DATASET = []
        for i, dct in enumerate(DATA_CATALOG_INF['DATASET']):
            df_lt = []
            tmp_lt = []
            for key, value in dct.items():
                if type(value) == dict:
                    df_tmp1 = pd.DataFrame(value, index=[i])
                    df_tmp1.columns = [key + '_' + c1 for c1 in df_tmp1.columns]
                    df_lt += [df_tmp1]
                else:
                    tmp_lt += [[key, value]]
                df_tmp1 = pd.concat(df_lt, axis=1)
                df_tmp2 = pd.DataFrame(tmp_lt, columns=['key', i])
                df_tmp2 = df_tmp2.set_index('key')
                df_tmp2 = df_tmp2.T
            DATASET += [pd.concat([df_tmp1, df_tmp2], axis=1)]
        DATASET = pd.concat(DATASET, axis=0)
    
        return DATASET, CATAROG_id, STATUS, DATE, NUMBER, RESULT_INF

#%%
    # e-Statのメタ情報取得
    def get_estat_MetaInfo(self, statsDataId):
        """
        メタ情報取得
        
        Parameters
        ----------
        statsDataId : string
            「統計表情報取得」で得られる統計表IDを指定.
    
        Returns
        -------
        TABLE_INF : dict
            指定した統計表の情報を出力します。属性として統計表ID(id)を保持.
            
        CLASS_INF : dict
            統計データのメタ情報を出力.  
            
        STATUS : int
            0～2の場合は正常終了、100以上の場合はエラー.
            ['GET_META_INFO']['RESULT']['STATUS'] 
            
        DATE : date
            このJSONデータが出力された日時.
            ['GET_META_INFO']['RESULT']['DATE'] 
        
        Reference
        -------
        出力しない項目
        ['GET_META_INFO']['RESULT']['ERROR_MSG'] : STATUSの値に対応するエラーメッセージです。
        ['GET_META_INFO']['PARAMETER']['LANG'] : 言語
        ['GET_META_INFO']['PARAMETER']['STATS_DATA_ID'] : statsDataId
        ['GET_META_INFO']['PARAMETER']['DATA_FORMAT'] : 出力フォーマット形式「X」：XML形式「J」：JSON形式又はJSONP形式
    
        """
        request_url_str = "https://api.e-stat.go.jp/rest/%s/app/json/getMetaInfo?" % str(self.version)
        params = {
            "appId": self.appId,
            "statsDataId": statsDataId
        }
        params_str = urllib.parse.urlencode(query=params)  # urllib
        request_url_str += params_str
        MetaInfo = requests.get(request_url_str).json()
        
        TABLE_INF = MetaInfo['GET_META_INFO']['METADATA_INF']['TABLE_INF']
        CLASS_INF = MetaInfo['GET_META_INFO']['METADATA_INF']['CLASS_INF']
        STATUS = MetaInfo['GET_META_INFO']['RESULT']['STATUS']
        DATE = MetaInfo['GET_META_INFO']['RESULT']['DATE']
    
        return TABLE_INF, CLASS_INF, STATUS, DATE




#%%
    # e-StatAPIから統計データをJSON形式でデータを取得
    def get_estat_StatsData(self, statsDataId, 
                            lvTab=None, cdTab=None, cdTabFrom=None, cdTabTo=None, 
                            lvTime=None, cdTime=None, cdTimeFrom=None, cdTimeTo=None, 
                            lvArea=None, cdArea=None, cdAreaFrom=None, cdAreaTo=None, 
                            lvCat01=None, cdCat01=None, cdCat01From=None, cdCat01To=None, 
                            lvCat02=None, cdCat02=None, cdCat02From=None, cdCat02To=None, 
                            lvCat03=None, cdCat03=None, cdCat03From=None, cdCat03To=None, 
                            startPosition=None, limit=100000, 
                            metaGetFlg=None, cntGetFlg=None, version='3.0'):
        """
        e-StatAPIから統計データをJSON形式でデータを取得
        
        Parameters
        ----------
        appId : string
            取得したアプリケーションIDを指定.
        statsDataId : string
            「統計表情報取得」で得られる統計表IDを指定.
        limit : int
            データセット取得件数
        version : string, float
            e-Stat APIのバージョン. The default is '3.0'.
    
        Returns
        -------
        json : dict
            
        
        """
        request_url_str = 'https://api.e-stat.go.jp/rest/%s/app/json/getStatsData?' % self.version
        self.statsDataId = statsDataId
        params = {
            'appId': self.appId,
            'statsDataId': self.statsDataId,
            'limit': limit
        }
        # 絞り込み条件
        # 表章事項
        if (type(lvTab) == int) | (type(lvTab) == str):  # 階層level Table
            params.update({'lvTab': lvTab})
        if (type(cdTab) == int) | (type(cdTab) == str):  # コードcode Table
            params.update({'cdTab': cdTab})
        if (type(cdTabFrom) == int) | (type(cdTabFrom) == str):
            params.update({'cdTabFrom': cdTabFrom})
        if (type(cdTabTo) == int) | (type(cdTabTo) == str):
            params.update({'cdTabTo': cdTabTo})
        # 時間軸事項
        if (type(lvTime) == int) | (type(lvTime) == str):  # 階層level
            params.update({'lvTime': lvTime})
        if (type(cdTime) == int) | (type(cdTime) == str):  # コードcode
            params.update({'cdTime': cdTime})
        if (type(cdTimeFrom) == int) | (type(cdTimeFrom) == str):
            params.update({'cdTimeFrom': cdTimeFrom})
        if (type(cdTimeTo) == int) | (type(cdTimeTo) == str):
            params.update({'cdTimeTo': cdTimeTo})
        # 地域事項
        if (type(lvArea) == int) | (type(lvArea) == str):  # 階層level
            params.update({'lvArea': lvArea})
        if (type(cdArea) == int) | (type(cdArea) == str):  # コードcode
            params.update({'cdArea': cdArea})
        if (type(cdAreaFrom) == int) | (type(cdAreaFrom) == str):
            params.update({'cdAreaFrom': cdAreaFrom})
        if (type(cdAreaTo) == int) | (type(cdAreaTo) == str):
            params.update({'cdAreaTo': cdAreaTo})
        # 分類事項1
        if (type(lvCat01) == int) | (type(lvCat01) == str):  # 階層level Category
            params.update({'lvCat01': lvCat01})
        if (type(cdCat01) == int) | (type(cdCat01) == str):  # コードcode Category
            params.update({'cdCat01': cdCat01})
        if (type(cdCat01From) == int) | (type(cdCat01From) == str):
            params.update({'cdCat01From': cdCat01From})
        if (type(cdCat01To) == int) | (type(cdCat01To) == str):
            params.update({'cdCat01To': cdCat01To})
        # 分類事項2
        if (type(lvCat02) == int) | (type(lvCat02) == str):  # 階層level Category
            params.update({'lvCat02': lvCat02})
        if (type(cdCat02) == int) | (type(cdCat02) == str):  # コードcode Category
            params.update({'cdCat02': cdCat02})
        if (type(cdCat02From) == int) | (type(cdCat02From) == str):
            params.update({'cdCat02From': cdCat02From})
        if (type(cdCat02To) == int) | (type(cdCat02To) == str):
            params.update({'cdCat02To': cdCat02To})
        # 分類事項3
        if (type(lvCat03) == int) | (type(lvCat03) == str):  # 階層level Category
            params.update({'lvCat03': lvCat03})
        if (type(cdCat03) == int) | (type(cdCat03) == str):  # コードcode Category
            params.update({'cdCat03': cdCat03})
        if (type(cdCat03From) == int) | (type(cdCat03From) == str):
            params.update({'cdCat03From': cdCat03From})
        if (type(cdCat03To) == int) | (type(cdCat03To) == str):
            params.update({'cdCat03To': cdCat03To})
        # データ取得開始位置
        if (type(startPosition) == int) | (type(startPosition) == str):
            params.update({'startPosition': startPosition})
        # メタ情報有無
        if (metaGetFlg == 'Y') or (metaGetFlg == 'N'):
            params.update({'metaGetFlg': metaGetFlg})
        # 件数取得フラグ
        if (cntGetFlg == 'Y') or (cntGetFlg == 'N'):
            params.update({'cntGetFlg': cntGetFlg})
        
        params_str = urllib.parse.urlencode(query=params)  # urllib
        request_url_str += params_str
        self.json = requests.get(request_url_str).json()  # requests
        return self

#%%
    # データを取得できているか確認
    def estat_json_check(self, metaGetFlg=None):
        """
        Parameters
        ----------
        json : dict
            get_estat_StatsData().
        statsDataId : string
            「統計表情報取得」で得られる統計表IDを指定.
        metaGetFlg : str
            メタ情報有無.
            Y:取得する(省略値)
            N:取得しない
    
        Returns
        -------
        TOTAL_NUMBER : int
            絞込条件に一致する統計データの件.
        DATA_NAME : str
            取得した統計データの名称.
            
        STATUS : int
            0～2の場合は正常終了、100以上の場合はエラー.
            ['GET_STATS_DATA']['RESULT']['STATUS'] 
            
        DATE : date
            このJSONデータが出力された日時.
            ['GET_STATS_DATA']['RESULT']['DATE'] 
            
    
        """
        if metaGetFlg != 'N':
            self.STATUS = self.json['GET_STATS_DATA']['RESULT']['STATUS']
            self.DATE = self.json['GET_STATS_DATA']['RESULT']['DATE'] 
            if (self.STATUS == 0) | (self.STATUS == 1):
                self.TOTAL_NUMBER = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['RESULT_INF']['TOTAL_NUMBER']  # レコード数
                if self.TOTAL_NUMBER > 100000:
                    print(str(self.TOTAL_NUMBER) + '行のうち100000行を取得しました。')
                else:
                    print(str(self.TOTAL_NUMBER) + '行を取得しました。')
                
                STAT_NAME = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['TABLE_INF']['STAT_NAME']['$']
                STATISTICS_NAME = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['TABLE_INF']['STATISTICS_NAME'].replace(' ', '')
                TITLE = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['TABLE_INF']['TITLE']
                if type(TITLE) == str:
                    TITLE = TITLE.replace(' ', '')
                else:
                    TITLE = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['TABLE_INF']['TITLE']['$'].replace(' ', '')
                CYCLE = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['TABLE_INF']['CYCLE']
                self.DATA_NAME = self.statsDataId + '_' + STAT_NAME + '_' + STATISTICS_NAME + '_' + TITLE + '_' + CYCLE
                return self
            else:
                print(self.json['GET_STATS_DATA']['RESULT']['ERROR_MSG'])
        else:
            print('metaGetFlgがNです。')
    
#%%
    # 属性マスタと結合しDataFrame形式に変換
    def estat_json_to_df(self, fillna='NULL'):
        """
        Parameters
        ----------
        json : dict
            get_estat_StatsData().
    
        Returns
        -------
        data_value :  pandas.core.frame.DataFrame
            統計数値(セル)の情報と項目名.データ件数分だけ出力.
    
        """
        self.data_value = pd.DataFrame(self.json['GET_STATS_DATA']['STATISTICAL_DATA']['DATA_INF']['VALUE'])
        if self.data_value.shape[0] == 100000:
            print('行数が100000行です。すべてのデータを取得できていない可能性があります。')
        self.data_value.columns = [col.replace('@', '') for col in self.data_value.columns]
        cond = (self.data_value['$']=='-') | (self.data_value['$']=='…') | (self.data_value['$']=='･･･') | (self.data_value['$']=='X')
        self.data_value['$'] = self.data_value['$'].mask(cond, fillna)
        
        # コードをキーとしてマスタテーブルと結合
        lst = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['CLASS_INF']['CLASS_OBJ']        
        for i, dct in enumerate(lst):
            if type(dct['CLASS']) == list:
                try:
                    tmp_df = pd.DataFrame(dct['CLASS'])[['@code', '@name', '@level', '@parentCode']]
                except:
                    tmp_df = pd.DataFrame(dct['CLASS'])[['@code', '@name', '@level']]
                tmp_df.columns = [col.replace('@', '')+'_'+dct['@id']+'_'+dct['@name'] for col in tmp_df.columns]
                tmp_df = tmp_df.rename(columns={'name_'+dct['@id']+'_'+dct['@name']: dct['@name']})
            else:
                tmp_S = pd.Series(dct['CLASS'])[['@code', '@name']]
                tmp_S.index = ['code_'+dct['@id']+'_'+dct['@name'], dct['@name']]
                #tmp_S.index = [idx.replace('@', '')+'_'+dct['@id']+'_'+dct['@name'] for idx in tmp_S.index]
                tmp_df = pd.DataFrame(tmp_S).T
            self.data_value = self.data_value.merge(tmp_df, left_on=dct['@id'], right_on='code_'+dct['@id']+'_'+dct['@name'], how='left')
            self.data_value['code_name_'+dct['@id']+'_'+dct['@name']] = self.data_value['code_'+dct['@id']+'_'+dct['@name']] + '_' + self.data_value[dct['@name']]
            self.data_value = self.data_value.drop('code_'+dct['@id']+'_'+dct['@name'], axis=1)
        return self

#%%
    def get_estat_StatsData_df(self, statsDataId, 
                            lvTab=None, cdTab=None, cdTabFrom=None, cdTabTo=None, 
                            lvTime=None, cdTime=None, cdTimeFrom=None, cdTimeTo=None, 
                            lvArea=None, cdArea=None, cdAreaFrom=None, cdAreaTo=None, 
                            lvCat01=None, cdCat01=None, cdCat01From=None, cdCat01To=None, 
                            lvCat02=None, cdCat02=None, cdCat02From=None, cdCat02To=None, 
                            lvCat03=None, cdCat03=None, cdCat03From=None, cdCat03To=None, 
                            startPosition=None, limit=100000, 
                            metaGetFlg=None, cntGetFlg=None, version='3.0'):
        """
        e-StatAPIから統計データをJSON形式でデータを取得
        
        Parameters
        ----------
        appId : string
            取得したアプリケーションIDを指定.
        statsDataId : string
            「統計表情報取得」で得られる統計表IDを指定.
        limit : int
            データセット取得件数
        version : string, float
            e-Stat APIのバージョン. The default is '3.0'.
    
        Returns
        -------
        data_value :  pandas.core.frame.DataFrame
            統計数値(セル)の情報と項目名.データ件数分だけ出力.
        TOTAL_NUMBER : int
            絞込条件に一致する統計データの件.
        DATA_NAME : str
            取得した統計データの名称.
        STATUS : int
            0～2の場合は正常終了、100以上の場合はエラー.
            ['GET_STATS_DATA']['RESULT']['STATUS'] 
        DATE : date
            このJSONデータが出力された日時.
            ['GET_STATS_DATA']['RESULT']['DATE'] 
        
        """
        self.get_estat_StatsData(statsDataId, 
                            lvTab, cdTab, cdTabFrom, cdTabTo, 
                            lvTime, cdTime, cdTimeFrom, cdTimeTo, 
                            lvArea, cdArea, cdAreaFrom, cdAreaTo, 
                            lvCat01, cdCat01, cdCat01From, cdCat01To, 
                            lvCat02, cdCat02, cdCat02From, cdCat02To, 
                            lvCat03, cdCat03, cdCat03From, cdCat03To, 
                            startPosition, limit, 
                            metaGetFlg, cntGetFlg, version)
            
        self.estat_json_check(metaGetFlg)
        
        self.estat_json_to_df()
        
        return self


#%%
    ## データが10万件を超える場合の一括処理
    def get_estat_StatsData_df_unlimitTime(self, statsDataId, cdTime=1985):
        """
        年で2020年から1985年までで分割する。
        """
        self.get_estat_StatsData(statsDataId)
        TOTAL_NUMBER = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['RESULT_INF']['TOTAL_NUMBER']
        df_lt = []
        if TOTAL_NUMBER > 100000:
            for t in range(2020, cdTime, -1):
                try:
                    self.get_estat_StatsData_df(statsDataId, cdTimeFrom=t-1, cdTimeTo=t)  # cdTimeTo未満
                    if self.STATUS == 0:
                        df_lt += [self.data_value]
                        time.sleep(1)
                except:
                    pass
    
        self.data_value = pd.concat(df_lt, axis=0)
        return self

#%%
    ## データが10万件を超える場合の一括処理
    def get_estat_StatsData_df_unlimitArea(self, statsDataId):
        """
        年で2020年から1985年までで分割する。
        """
        self.get_estat_StatsData(statsDataId)
        TOTAL_NUMBER = self.json['GET_STATS_DATA']['STATISTICAL_DATA']['RESULT_INF']['TOTAL_NUMBER']
        df_lt = []
        if TOTAL_NUMBER > 100000:
            for ca in [('0' + str(i) + '000')[-5:] for i in range(48)]:
                try:
                    self.get_estat_StatsData_df(statsDataId, cdArea=ca)  # cdTimeTo未満
                    if self.STATUS == 0:
                        df_lt += [self.data_value]
                        time.sleep(1)
                except:
                    pass
    
        self.data_value = pd.concat(df_lt, axis=0)
        return self

#%%
    def tab_pivot(self, to_numeric=False):
        self.data_value['code_name_tab_表章項目_unit_level'] = self.data_value['code_name_tab_表章項目'] + '(' + self.data_value['unit'].fillna('') + ')'
        if 'level_tab_表章項目' in self.data_value.columns:
            self.data_value['code_name_tab_表章項目_unit_level'] = self.data_value['code_name_tab_表章項目_unit_level'] + self.data_value['level_tab_表章項目']
        self.data_value['code_name_tab_表章項目_unit_level'] = self.data_value['code_name_tab_表章項目_unit_level'].str.replace('()', '', regex=False)
        self.data_value = self.data_value.drop(['tab', '表章項目', 'code_name_tab_表章項目', 'unit'], axis=1)
        if 'level_tab_表章項目' in self.data_value.columns:
            self.data_value = self.data_value.drop('level_tab_表章項目', axis=1)
        cols_lst = list(self.data_value.columns)
        cols_lst.remove('$')
        df_tab = self.data_value.set_index(cols_lst).unstack()
        df_tab.columns = [j for i, j in df_tab.columns]
        if to_numeric:
            for c in df_tab:
                try:
                    df_tab[c] = df_tab[c].astype(int)
                    print(c, 'をint型に変換しました')
                except:
                    try:
                        df_tab[c] = df_tab[c].astype(float)
                        print(c, 'をfloat型に変換しました')
                    except:
                        print(c, 'はint,floatに変換できません')
        self.data_value = df_tab.reset_index()
        return self
