import tarfile as tf
from prefect import flow, task
from pathlib import Path
import requests
import json
import pandas as pd
import datetime
from dependencies.dataframe_extras import save_or_upload_dataframe

@flow(name="Extract League Assets", log_prints=True)
def get_league_assets(version, language = "en_PH"):
    """
        Complete ETL flow to save all data related to the ETL process.
        Args:
            version (str): League of Legends version to get data from.
            language (str): Language to get data from. Default: PH - English
        Returns:
            None
    """
    #Get tarfile from HTTP get:
    data_dragon_file = f"https://ddragon.leagueoflegends.com/cdn/dragontail-{version}.tgz"
    data_dragon_local_file = Path(f"resources/data_dragon_{version}.tgz")
    data_dragon_local_folder = Path(f"resources/data_dragon_{version}")

    if not data_dragon_local_file.exists():
        download_data_dragon_file(data_dragon_file, data_dragon_local_file)
    
    if not data_dragon_local_folder.exists():
        extract_tarfile(data_dragon_local_file, data_dragon_local_folder)

    champion_infos : pd.DataFrame = getChampionInfo(version, language)
    map_infos : pd.DataFrame = getMapInfo(version, language)
    item_infos : pd.DataFrame = getItemInfo(version, language, map_infos)
    rune_infos : pd.DataFrame = getRuneInfo(version, language)
    summoner_spells_info : pd.DataFrame = getSummonerSpellsInfo(version, language)

    save_or_upload_dataframe(champion_infos, Path(f"data_dragon/{version}"), f"champion_infos.parquet")
    save_or_upload_dataframe(map_infos, Path(f"data_dragon/{version}"), f"map_infos.parquet")
    save_or_upload_dataframe(item_infos, Path(f"data_dragon/{version}"), f"item_infos.parquet")
    save_or_upload_dataframe(rune_infos, Path(f"data_dragon/{version}"), f"rune_infos.parquet")
    save_or_upload_dataframe(summoner_spells_info, Path(f"data_dragon/{version}"), f"summoner_spells_info.parquet")

@task(name = "Get Map Info", log_prints=True)
def getMapInfo(version: str, language: str) -> pd.DataFrame:
    item_infos_raw = json.load(open(f"resources/data_dragon_{version}/{version}/data/{language}/map.json", encoding='utf-8'))
    map_infos = []

    for map_id in item_infos_raw["data"].keys():
        print(f"Getting map info for:  {map_id}")
        map_info = {}
        map_info["version"] = item_infos_raw["version"]
        map_info["id"] = map_id

        map = item_infos_raw["data"][map_id]
        map_info["MapName"] = map["MapName"]
        map_info["image"] = map["image"]["full"]
        map_infos.append(map_info)

    return pd.DataFrame(map_infos)

@task(name = "Get Item Info", log_prints=True)
def getItemInfo(version: str, language: str, map_info : pd.DataFrame) -> pd.DataFrame:
    print(f"Getting item infos")

    item_infos_raw = json.load(open(f"resources/data_dragon_{version}/{version}/data/{language}/item.json", encoding='utf-8'))
    item_infos = []

    map_id_map = map_info.set_index("id").replace(' ','', regex=True).replace("\'", regex=True).to_dict()["MapName"]

    tag_columns = set()
    stats_columns = set()

    for item_id in item_infos_raw["data"].keys():
        
        item_info = {}
        item_info["version"] = item_infos_raw["version"]
        item_info["id"] = item_id

        item = item_infos_raw["data"][item_id]
        item_info["name"] = item["name"]

        if item.get("rune") is not None:
            item_info["isItemRune"] = item.get("rune").get("isrune")
            item_info["itemRuneTier"] = item.get("rune").get("tier")
            item_info["itemRuneType"] = item.get("rune").get("type")

        item_info["baseGold"] = item.get("gold").get("base")
        item_info["sellGold"] = item.get("gold").get("sell")
        item_info["totalGold"] = item.get("gold").get("total")
        item_info["isGoldPurchasable"] = item.get("gold").get("purchasable")
   
        item_info["description"] = item.get("description")
        item_info["colloq"] = item.get("colloq")
        item_info["plaintext"] = item.get("plaintext")
   
        item_info["maximumStacks"] = item.get("stacks")
        item_info["depth"] = item.get("depth")

        item_info["isConsumable"] = item.get("consumed")
        item_info["onlyConsumeOnFull"] = item.get("consumeOnFull")

        if item.get("from") is not None:
            item_info["buildsFrom"] = ",".join(map(str, item.get("from")))

        
        if item.get("into") is not None:
            item_info["buildsInto"] = ",".join(map(str, item.get("into")))

        item_info["specialRecipe"] = item.get("specialRecipe")
        item_info["inStore"] = item.get("inStore") if item.get("inStore") is not None else True
        item_info["hideFromAll"] = item.get("hideFromAll") if item.get("hideFromAll") is not None else False

        item_info["requiredChampion"] = item.get("requiredChampion")
        item_info["requiredAlly"] = item.get("requiredAlly")

        for stat in item.get("stats").keys():
            item_info[f"stat_{stat}"] = item.get("stats")[stat]
            stats_columns.add(f"stat_{stat}")

        for map_id in item.get("maps"):
            item_info[f"availableIn{map_id_map[map_id]}"] = item.get("maps").get(map_id)

        
        for tag in item.get("tags"):
            item_info[f"itemIs{tag}"] = True
            tag_columns.add(f"itemIs{tag}")

        item_infos.append(item_info)


    item_infos = pd.DataFrame(item_infos)
    item_infos.loc[:, list(tag_columns)] = item_infos[list(tag_columns)].fillna(False)
    item_infos.loc[:, list(stats_columns)] = item_infos[list(stats_columns)].fillna(0)

    # null_mask = item_infos.isna()
    # all_null_cols = null_mask.all(axis=0)
    # all_null_cols_list = all_null_cols.index[all_null_cols].tolist()
    # assert len(all_null_cols_list) == 0, f"Columns with all null values: {all_null_cols_list}"
    return item_infos

@task(name = "Get Champion Info", log_prints=True)
def getChampionInfo(version: str,language:str) -> pd.DataFrame:
    #Get champion info
    champion_infos_raw = json.load(open(f"resources/data_dragon_{version}/{version}/data/{language}/champion.json", encoding='utf-8'))
    champion_infos = []

    for champion in champion_infos_raw["data"].keys():
        print(f"Getting champion info for:  {champion}")
        champion_info = {}
        champion = champion_infos_raw["data"][champion]
        champion_info["version"] = champion["version"]
        champion_info["id"] = champion["id"]
        champion_info["key"] = champion["key"]
        champion_info["name"] = champion["name"]
        champion_info["title"] = champion["title"]
        champion_info["blurb"] = champion["blurb"]

        for stat in champion["info"].keys():
            champion_info[f"{stat}_stat"] = champion["info"][stat]

        champion_info["image"] = champion["image"]["full"]

        for i, tag in enumerate(champion["tags"]):
            if i == 0:
                champion_info["primary_class"] = tag
            elif i == 1:
                champion_info["secondary_class"] = tag
            else:
                champion_info[f"tag_{i}"] = tag
            

        champion_info["partype"] = champion["partype"]

        for stat in champion["stats"].keys():
            if "perlevel" in stat:
                champion_info[f"{stat}"] = champion["stats"][stat]
            else:
                champion_info[f"base_{stat}"] = champion["stats"][stat]
 
        champion_info["created_at"] = datetime.datetime.now()

        champion_infos.append(champion_info)

    # print(len(champion_info))
    return pd.DataFrame(champion_infos)

@task(name = "Get Runes Info", log_prints=True)
def getRuneInfo(version: str,language:str) -> pd.DataFrame:
    rune_infos_raw = json.load(open(f"resources/data_dragon_{version}/{version}/data/{language}/runesReforged.json", encoding='utf-8'))
    rune_infos = []


    for rune_path in rune_infos_raw:

        rune_info = {}
        rune_info["version"] = version
        rune_info["id"] = rune_path["id"]
        rune_info["key"] = rune_path["key"]
        rune_info["rune_path"] = rune_path["key"]
        rune_info["rune_path_icon"] = rune_path["icon"]
        rune_info["name"] = rune_path["name"]
        rune_infos.append(rune_info)
        del rune_info

        for rune_level, rune_slot in enumerate(rune_path["slots"]):
            for rune in rune_slot["runes"]:
                rune_info = {}
                rune_info["version"] = version
                rune_info["id"] = rune["id"]
                rune_info["key"] = rune["key"]
                rune_info["icon"] = rune["icon"]
                rune_info["name"] = rune["name"]
                rune_info["shortDesc"] = rune["shortDesc"]
                rune_info["longDesc"] = rune["longDesc"]
                rune_info["rune_level"] = rune_level+1
                rune_info["rune_path"] = rune_path["key"]
                rune_infos.append(rune_info)
                del rune_info
        
    return pd.DataFrame(rune_infos)

@task(name = "Get Summoner Spells Info", log_prints=True)
def getSummonerSpellsInfo(version: str,language:str) -> pd.DataFrame:
    summoner_spells_raw = json.load(open(f"resources/data_dragon_{version}/{version}/data/{language}/summoner.json", encoding='utf-8'))
    summoner_spells = []

    game_modes = set()
    for spell in summoner_spells_raw["data"].keys():
        print(f"Getting summoner spell info for:  {spell}")
        spell_info = {}
        spell = summoner_spells_raw["data"][spell]
        spell_info["version"] = summoner_spells_raw["version"]
        spell_info["id"] = spell["id"]
        spell_info["key"] = spell["key"]
        spell_info["name"] = spell["name"]
        spell_info["description"] = spell["description"]
        spell_info["tooltip"] = spell["tooltip"]
        spell_info["maxrank"] = spell["maxrank"]
        spell_info["cooldown"] = ",".join(map(str,spell["cooldown"]))
        spell_info["cooldownBurn"] = spell["cooldownBurn"]
        spell_info["cost"] = ",".join(map(str,spell["cost"]))
        spell_info["minimumSummonerLevel"] = spell["summonerLevel"]
        spell_info["costType"] = spell["costType"]
        spell_info["maxammo"] = spell["maxammo"]
        spell_info["range"] = spell["range"]
        spell_info["rangeBurn"] = spell["rangeBurn"]
        spell_info["image"] = spell["image"]["full"]
        spell_info["resource"] = spell["resource"]

        for modes in spell["modes"]:
            spell_info[f"availableIn{modes}"] = True
            game_modes.add(f"availableIn{modes}")


        summoner_spells.append(spell_info)
    
    summoner_spells = pd.DataFrame(summoner_spells)
    summoner_spells.loc[:, list(game_modes)] = summoner_spells[list(game_modes)].fillna(False)
    return pd.DataFrame(summoner_spells)

@task(name="Extract Tarball", log_prints=True)
def extract_tarfile(local_file: Path, local_folder:Path):
    print("Extracting tarball to: " + str(local_folder))
    with tf.open(str(local_file), "r:gz") as tar:
        tar.extractall(str(local_folder))

@task(name = "Download Data Dragon File", log_prints=True)
def download_data_dragon_file(url: str, local_file : str):
    print("Downloading file from: " + url)

    # Download file from URL
    response = requests.get(url)
    # Create file path
    with open(local_file, "wb") as f:
        f.write(response.content)


get_league_assets("13.7.1")