from setup import *
from WindPy import w


def convert_date_format_from_8_to_10(x):
    return "{}-{}-{}".format(x[0:4], x[4:6], x[6:8])


def download_security_id_by_date_wind(t_download_date: str, t_save_root_dir: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.csv.gz".format(t_download_date)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        downloaded_data = w.wset("sectorconstituent", "date={};sectorid=a001010100000000;field=wind_code".format(
            convert_date_format_from_8_to_10(t_download_date))
                                 )
        if downloaded_data.ErrorCode == 0:
            downloaded_df = pd.DataFrame({"wind_code": [z for z in downloaded_data.Data[0] if z[-2:] != "BJ"]})
            downloaded_df = downloaded_df.sort_values(by="wind_code", ascending=True)
            downloaded_df.to_csv(save_path, index=False)
            print("| {0} | {1} | {2:>7d} sec_id downloaded successfully |".format(dt.datetime.now(), t_download_date, len(downloaded_df)))
        else:
            print("| {0} | {1} | Error when download security id, Error Code = {2} |".format(dt.datetime.now(), t_download_date, downloaded_data.errorcode))

    return 0


def download_security_mkt_data_by_date_wind(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.md.csv.gz".format(t_download_date)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        data_set = {}
        error_code_list = []
        for variable in ["open", "high", "low", "close", "volume", "pre_close"]:
            downloaded_data = w.wsd(
                t_sec_id_list,
                variable,
                convert_date_format_from_8_to_10(t_download_date),
                convert_date_format_from_8_to_10(t_download_date),
                "")
            if downloaded_data.ErrorCode != 0:
                error_code_list.append(downloaded_data.ErrorCode)
                print("| {0} | {1} | Error when download security md = {2}, Error Code = {3} |".format(dt.datetime.now(), t_download_date, variable, downloaded_data.ErrorCode))
            else:
                downloaded_df = pd.DataFrame({"wind_code": downloaded_data.Codes, variable: downloaded_data.Data[0]}).set_index("wind_code")
                data_set[variable] = downloaded_df[variable]

        if len(error_code_list) == 0:
            merged_df = pd.DataFrame(data_set)
            merged_df["pct_chg"] = np.round((merged_df["close"] / merged_df["pre_close"] - 1) * 100, 4)
            merged_df["volume"] = merged_df["volume"].fillna(0)
            merged_df = merged_df[["open", "high", "low", "close", "volume", "pct_chg"]].sort_index(ascending=True)
            merged_df.to_csv(save_path, index_label="wind_code", float_format="%.4f", compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, "ohlcvpc"))
        else:
            print(error_code_list)
    return 0


def download_security_mkt_data_extra_by_date_wind(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_extra_var: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.md.{}.csv.gz".format(t_download_date, t_extra_var)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        t_extra_var_wind = {
            "money": "amt"
        }[t_extra_var]
        downloaded_data = w.wsd(
            t_sec_id_list,
            t_extra_var_wind,
            convert_date_format_from_8_to_10(t_download_date),
            convert_date_format_from_8_to_10(t_download_date),
            "")
        if downloaded_data.ErrorCode == 0:
            merged_df = pd.DataFrame({"wind_code": downloaded_data.Codes, t_extra_var: downloaded_data.Data[0]}).set_index("wind_code")
            merged_df = merged_df.sort_index(ascending=True)
            merged_df.to_csv(save_path, index_label="wind_code", float_format="%.4f", compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_extra_var))
        else:
            print("| {0} | {1} | Error when download security {2}, Error Code = {3} |".format(dt.datetime.now(), t_download_date, t_extra_var, downloaded_data.ErrorCode))
    return 0


def download_security_fundamental_by_date_wind(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_variable: str, t_skip_when_exists: bool):
    # available t_variable
    rename_mapper_ths_to_generic = {
        "mkt_cap_ard": "mkt_cap",
        "val_floatmv": "clc_mkt_cap",

        "val_pe_deducted_ttm": "pe",
        "pb_mrq": "pb",
        "free_turn_n": "to_rto"
    }
    rename_mapper_generic_to_wind = {v: k for k, v in rename_mapper_ths_to_generic.items()}

    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.{}.csv.gz".format(t_download_date, t_variable)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        downloaded_df = None
        t_variable_wind = rename_mapper_generic_to_wind.get(t_variable, None)
        if t_variable_wind is not None:
            downloaded_data = w.wsd(
                t_sec_id_list, t_variable_wind,
                convert_date_format_from_8_to_10(t_download_date), convert_date_format_from_8_to_10(t_download_date),
                # "unit=1"
            )
            if downloaded_data.ErrorCode == 0:
                downloaded_df = pd.DataFrame({"wind_code": downloaded_data.Codes, t_variable: downloaded_data.Data[0]}).set_index("wind_code")
            else:
                print("| {0} | {1} | Error when download security {2:}, Error Code = {3} |".format(
                    dt.datetime.now(), t_download_date, t_variable, downloaded_data.errorcode))

        if downloaded_df is not None:
            downloaded_df = downloaded_df.sort_index(ascending=True)
            save_precision = {
                "mkt_cap": "%.2f",
                "clc_mkt_cap": "%.2f",
                "pb": "%.6f",
                "pe": "%.6f",
                "to_rto": "%.6f",
            }[t_variable]
            downloaded_df.to_csv(save_path, index_label="wind_code", float_format=save_precision, compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_variable))

    return 0


def download_security_sector_by_date_wind(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_sector_class: str, t_skip_when_exists: bool):
    sector_sub_class_mgr = {
        "zjw": {"code": "industry_CSRCcode12", "name": "industry_csrc12_n", "type": 3},
        "sw_l1": {"code": None, "name": "industry_sw_2021", "type": 1},
        "sw_l2": {"code": None, "name": "industry_sw_2021", "type": 2},
        "sw_l3": {"code": None, "name": "industry_sw_2021", "type": 3},
    }

    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.sector.{}.csv.gz".format(t_download_date, t_sector_class)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        sector_df = None

        if t_sector_class in ["zjw"]:
            downloaded_data = w.wss(
                t_sec_id_list,
                "{},{}".format(sector_sub_class_mgr[t_sector_class]["code"], sector_sub_class_mgr[t_sector_class]["name"]),
                "tradeDate={};industryType={}".format(t_download_date, sector_sub_class_mgr[t_sector_class]["type"])
            )
            if downloaded_data.ErrorCode == 0:
                sector_df = pd.DataFrame(
                    {
                        "wind_code": downloaded_data.Codes,
                        "industry_code": downloaded_data.Data[0],
                        "industry_name": downloaded_data.Data[1],
                    },
                )
            else:
                print("| {0} | {1} | Error when download security sector {2:}, Error Code = {3} |".format(
                    dt.datetime.now(), t_download_date, t_sector_class, downloaded_data.ErrorCode))

        if t_sector_class in ["sw_l1", "sw_l2", "sw_l3"]:
            # w.wss("000001.SZ,300001.SZ,600000.SH", "industry_sw_2021","tradeDate=20220519;industryType=1")
            downloaded_data = w.wss(
                t_sec_id_list,
                "{}".format(sector_sub_class_mgr[t_sector_class]["name"]),
                "tradeDate={};industryType={}".format(t_download_date, sector_sub_class_mgr[t_sector_class]["type"])
            )
            if downloaded_data.ErrorCode == 0:
                sector_df = pd.DataFrame(
                    {
                        "wind_code": downloaded_data.Codes,
                        "industry_name": downloaded_data.Data[0],
                    },
                )

                name_to_code_df = pd.read_excel("industry_info.xlsx", sheet_name=t_sector_class, dtype=str)
                name_to_code_df["industry_code"] = name_to_code_df["index"].map(lambda z: z.replace(".SI", ""))
                name_to_code_df["industry_name"] = name_to_code_df["name"].map(lambda z: z.replace("(申万)", ""))
                name_to_code_df = name_to_code_df.set_index("industry_name")
                name_to_code_mapper = name_to_code_df["industry_code"].to_dict()

                sector_df["industry_code"] = sector_df["industry_name"].map(lambda z: name_to_code_mapper.get(z, ""))

            else:
                print("| {0} | {1} | Error when download security sector {2:}, Error Code = {3} |".format(
                    dt.datetime.now(), t_download_date, t_sector_class, downloaded_data.ErrorCode))

        if sector_df is not None:
            sector_df = sector_df[["wind_code", "industry_code", "industry_name"]].sort_values(by="wind_code", ascending=True)
            sector_df.to_csv(save_path, index=False, encoding="gb18030")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_sector_class))
    return 0
