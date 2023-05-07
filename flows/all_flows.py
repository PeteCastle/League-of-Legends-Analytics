

import json
import pandas as pd
import itertools
from prefect import flow
from dependencies.league_entries import getLeagueEntries
from dependencies.match_entries import getMultipleMatchInfo
from dependencies.method_ingestion import ingestMultiplePlayerEntries, ingestMultiplePlayerMatches
from dependencies.method_ingestion import ingestMultiplePlayerMasteryIds
from dependencies.dataframe_extras import cleanDataframe, save_or_upload_dataframe
from datetime import datetime
from pathlib import Path

@flow(name="Per League Complete ETL", log_prints=True)
def etl_per_league(
    queue: str,
    tier : str,
    division : str = "I",
    pages : int = 1,
    regions : list[str] = None,
    ACCOUNT_INPUT_LIMIT : int = None,
    MATCH_INPUT_LIMIT : int = None,
    API_KEYS : list[str] = None, 
    MAXIMUM_CONCURRENT_REQUESTS : int = 100,
):
    """
        Complete ETL flow for the League of Legends API.  Gets all the players list for a given queue, tier, and division, then gets all the matches for each player, 
        and then gets all the player information for each match.
        Args:
            queue (str): The queue type to filter for. (legal values: RANKED_SOLO_5x5, RANKED_FLEX_SR, RANKED_FLEX_TT)
            tier (str): The league tier to filter for. (legal values: challengerleagues, grandmasterleagues, masterleagues, DIAMOND, PLATINUM, GOLD, SILVER, BRONZE, IRON)
            division (str): The league division to filter for. (legal values: I, II, III, IV). (default: I)
            pages (int): The number of pages to return. (legal range: 1-100). (default: 1)
            regions (list[str]): A list of regions to filter for. Defaults to all regions if not specified.(legal values: ["ph2", "eun1", "euw1", "jp1", "kr", "la1", "la2", "na1", "oc1", "ru", "sg2", "th2", "tr1", "tw2", "vn2","br1"]).
            ACCOUNT_INPUT_LIMIT (int): The maximum number of account IDs to be sent to the API. Account IDs received from the API are not limited.(default: None)
            MATCH_INPUT_LIMIT (int): The maximum number of match IDs to be sent to the API. Match IDs received from the API are not limited.(default: None)
            API_KEYS (list[str]): A list of API keys to use for the request. If None, the program will use the API_KEYS provided in ../configs/flows_config.ini.  Useful if the user has an application API. (default: None)
            MAXIMUM_CONCURRENT_REQUESTS (int): The maximum number of concurrent requests to make. If instabilities or crashes occur, lower the maximum amount.(default: 100)
        Returns:
            None
    """

    #Configuration
    config = json.load(open('configs/flows_config.json'))
    credentials = json.load(open('credentials/credentials.json'))
    if API_KEYS is None:
        API_KEYS = credentials["API_KEYS"]
        if len(API_KEYS) == 0:
            raise ValueError("No Riot API keys provided.")
    REGION_GROUPINGS = config["region_groupings"]
    if regions is None:
        regions = list(REGION_GROUPINGS.keys())
    

    #League Entries
    league_info, player_league_info = getLeagueEntries(queue=queue, tier=tier, division=division, pages=pages, regions = regions, RIOT_TOKENS=API_KEYS)

    #Player Entries
    player_leagues_tuple = list(player_league_info[["region","summonerId","riot_token"]].itertuples(index=False, name=None))
    player_info = ingestMultiplePlayerEntries(player_leagues_tuple, ACCOUNT_INPUT_LIMIT, MAXIMUM_CONCURRENT_REQUESTS)
    del player_leagues_tuple

    #Accounts Info
    print("Merging player info and league info with columns")
    accounts_info = pd.DataFrame(player_info).merge(player_league_info, left_on='id', right_on = 'summonerId', how='left')
    accounts_info["region_group"] = accounts_info["region"].map(REGION_GROUPINGS)

    #Player's Match History
    player_infos_tuple =list(accounts_info[['puuid','region_group','riot_token']].itertuples(index=False, name=None))
    match_history_raw = ingestMultiplePlayerMatches(player_infos_tuple,MAXIMUM_CONCURRENT_REQUESTS, ACCOUNT_INPUT_LIMIT )
    del player_infos_tuple

    #Match History Info
    # match_general_info, match_players_info, match_teams_info, match_players_challenges_info = getMultipleMatchInfo(match_history_raw, MATCH_INPUT_LIMIT, API_KEYS, MAXIMUM_CONCURRENT_REQUESTS, REGION_GROUPINGS)
    match_general_info, match_players_info, match_teams_info, match_players_challenges_info = getMultipleMatchInfo(match_history_raw, API_KEYS,MAXIMUM_CONCURRENT_REQUESTS,REGION_GROUPINGS,MATCH_INPUT_LIMIT)
    # Player Mastery Info
    player_infos_tuple = list(accounts_info[['id','region','riot_token']].itertuples(index=False, name=None))
    player_champion_mastery_info = itertools.chain.from_iterable(ingestMultiplePlayerMasteryIds(player_infos_tuple, MAXIMUM_CONCURRENT_REQUESTS, ACCOUNT_INPUT_LIMIT))
    player_champion_mastery_info = pd.DataFrame(player_champion_mastery_info).merge(accounts_info[['summonerId','summonerName','region']], on='summonerId', how='left')

    del player_infos_tuple

    #Clean dataframe
    league_info = cleanDataframe(league_info, "league_info")
    accounts_info = cleanDataframe(accounts_info, "accounts_info")
    match_general_info = cleanDataframe(match_general_info, "match_general_info")
    match_players_info = cleanDataframe(match_players_info, "match_players_info")
    match_players_challenges_info = cleanDataframe(match_players_challenges_info, "match_players_challenges_info")
    match_teams_info = cleanDataframe(match_teams_info, "match_teams_info")
    player_champion_mastery_info = cleanDataframe(player_champion_mastery_info, "player_champion_mastery_info")

    save_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_or_upload_dataframe(league_info,Path(f"leagues/{queue}/{tier}/{division}/"),f"{save_time}.csv")
    save_or_upload_dataframe(accounts_info,Path(f"accounts/"),f"{save_time}.csv")
    save_or_upload_dataframe(match_general_info,Path(f"match/general/{queue}/{tier}/{division}"),f"{save_time}.csv")
    save_or_upload_dataframe(match_players_info,Path(f"match/players/{queue}/{tier}/{division}/"),f"{save_time}.csv")
    save_or_upload_dataframe(match_teams_info,Path(f"match/teams/{queue}/{tier}/{division}/"),f"{save_time}.csv")
    save_or_upload_dataframe(match_players_challenges_info,Path(f"match/players_challenges/{queue}/{tier}/{division}/"),f"{save_time}.csv")
    save_or_upload_dataframe(player_champion_mastery_info,Path(f"champion_mastery/"),f"{save_time}.csv")
  
@flow(name = "All Leagues Complete ETL", log_prints = True)
def etl_all_league(
    queue: str,
    pages : int = 1,
    regions : list[str] = None,
    ACCOUNT_INPUT_LIMIT : int = None,
    MATCH_INPUT_LIMIT : int = None,
    API_KEYS : list[str] = None, 
    MAXIMUM_CONCURRENT_REQUESTS : int = 500,
):
    """
        Complete all flow, but for all leagues.
        Args:
            queue (str): The queue type to filter for. (legal values: RANKED_SOLO_5x5, RANKED_FLEX_SR, RANKED_FLEX_TT)
            pages (int): The number of pages to return. (legal range: 1-100). (default: 1)
            regions (list[str]): A list of regions to filter for. Defaults to all regions if not specified.(legal values: ["ph2", "eun1", "euw1", "jp1", "kr", "la1", "la2", "na1", "oc1", "ru", "sg2", "th2", "tr1", "tw2", "vn2","br1"]).
            ACCOUNT_INPUT_LIMIT (int): The maximum number of account IDs to be sent to the API. Account IDs received from the API are not limited.(default: None)
            MATCH_INPUT_LIMIT (int): The maximum number of match IDs to be sent to the API. Match IDs received from the API are not limited.(default: None)
            API_KEYS (list[str]): A list of API keys to use for the request. If None, the program will use the API_KEYS provided in ../configs/flows_config.ini.  Useful if the user has an application API. (default: None)
            MAXIMUM_CONCURRENT_REQUESTS (int): The maximum number of concurrent requests to make. If instabilities or crashes occur, lower the maximum amount.(default: 100)
        Returns:
            None
    """
    for tier in ["challengerleagues", "grandmasterleagues", "masterleagues","DIAMOND","PLATINUM","GOLD","SILVER","BRONZE", "IRON"]:
        if tier in ["challengerleagues", "grandmasterleagues", "masterleagues"]:
            print(f"Starting {tier} ETL")
            etl_per_league(queue, tier, "I", pages, regions, ACCOUNT_INPUT_LIMIT, MATCH_INPUT_LIMIT, API_KEYS, MAXIMUM_CONCURRENT_REQUESTS)
        else:
            for division in ["I", "II", "III", "IV"]:
                print(f"Starting {tier} {division} ETL")
                etl_per_league(queue, tier, division, pages, regions, ACCOUNT_INPUT_LIMIT, MATCH_INPUT_LIMIT, API_KEYS, MAXIMUM_CONCURRENT_REQUESTS)
    

etl_all_league("RANKED_SOLO_5x5",1, ACCOUNT_INPUT_LIMIT=2000,MATCH_INPUT_LIMIT = 2000, MAXIMUM_CONCURRENT_REQUESTS = 500)
