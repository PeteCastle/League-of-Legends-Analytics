from typing import Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from prefect import flow

from dependencies.method_ingestion import ingestLeagueEntries, ingestTopLeagueEntries

@flow(name="getTopLeagueEntries")
def getTopLeagueEntries(queue: str, tier :str, region, RIOT_TOKENS: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
    riot_token : str = np.random.choice(RIOT_TOKENS)
    league_entries_raw : dict = ingestTopLeagueEntries(queue, tier, region, riot_token)

    if not league_entries_raw:
        return pd.DataFrame(), pd.DataFrame()
    
    current_time = datetime.now()

    player_league_infos = pd.DataFrame(league_entries_raw["entries"])
    player_league_infos["leagueId"] = league_entries_raw.get("leagueId")
    player_league_infos["last_updated"] = datetime.now()
    player_league_infos["riot_token"] = riot_token
    player_league_infos["region"] = region
    # player_league_infos = reorderColumns(player_league_infos, ordered_columns= ["leagueId","region","summonerId","summonerName","leaguePoints","rank","wins","losses","veteran","inactive","freshBlood","hotStreak","last_updated","riot_token"])

    del league_entries_raw["entries"]
    league_infos = pd.DataFrame([league_entries_raw])
    league_infos["last_updated"] = datetime.now()
    league_infos["queue"] = queue
    league_infos["region"] = region
    league_infos["division"] = 'I'
    league_infos["riot_token"] = riot_token
    # league_infos = reorderColumns(league_infos, ordered_columns= ["leagueId","region","queue","tier","division","name","last_updated","riot_token"])
    return league_infos, player_league_infos

@flow(name="getLeagueEntries")
def getNormalLeagueEntries(queue: str, tier :str, division:str, region : str, pages : int, RIOT_TOKENS: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
    player_leauge_infos_list = []
    league_infos_list = []
    for page in range(1, pages+1):
        riot_token : str = np.random.choice(RIOT_TOKENS)
     
        league_entries_raw : dict = pd.DataFrame.from_dict(ingestLeagueEntries(queue, tier, division, region, riot_token, page))

        if league_entries_raw.empty:
            return pd.DataFrame(), pd.DataFrame()
 
        player_league_infos = league_entries_raw.loc[:,['leagueId','summonerId','summonerName','leaguePoints','rank','wins','losses','veteran','inactive','freshBlood','hotStreak']]
        player_league_infos["last_updated"] = datetime.now()
        player_league_infos["riot_token"] = riot_token
        player_league_infos["region"] = region
        player_leauge_infos_list.append(player_league_infos)
        # player_league_infos = reorderColumns(player_league_infos,["leagueId","region","summonerId","summonerName","leaguePoints","rank","wins","losses","veteran","inactive","freshBlood","hotStreak","last_updated","riot_token"])

        league_infos = league_entries_raw.loc[:,['leagueId','tier']]
        league_infos["region"] = region
        league_infos["queue"] = queue
        league_infos["division"] = league_entries_raw["rank"]
        league_infos["last_updated"] = datetime.now()
        league_infos["riot_token"] = riot_token
        # league_infos = reorderColumns(league_infos, ordered_columns= ["leagueId","region","queue","tier","division","name","last_updated","riot_token"])
        league_infos.drop_duplicates(subset=["leagueId"], inplace=True)
        league_infos_list.append(league_infos)

    return pd.concat(league_infos_list, axis=0), pd.concat(player_leauge_infos_list,axis=0)

@flow(name="all_league_entries")
def getLeagueEntries(queue : str,
                   tier : str, 
                   RIOT_TOKENS: list,
                   division: str = None,
                   regions: list[str] = "ph2",
                   pages : int = 1):
    """
        Main function for ingestion of data for leagues.
        Args:
            queue::str:
                The queue type of the league.  All valid values accepted according to API: RANK_FLEX_SR, RANK_FLEX_TT, RANKED_SOLO_5x5. Note that RANK_FLEX_TT is deprecated.
            tier::str:
                The league tier.  The following values are accepted: challengerleagues, grandmasterleagues, masterleagues, DIAMOND, PLATINUM, GOLD, SILVER, BRONZE, IRON
            division::str:
                The league division.  All valid values accepted according to API: I, II, III, IV.  Challenger to Master divisions only have one division.
            region::str:
                The region of the league. Default: All regions.  All valid values accepted according to API: ph2, eun1, euw1, jp1, kr, la1, la2, na1, oc1, ru, sg2, th2, tr1, tw2, vn2
            pages::int:
                The number of pages to be ingested.  Each page contains >20 entries and varies per region. Challenger to Master divisions always show the complete list (only 1 page).  
        Returns:
            None
    """
    print(f"Ingesting League Entries in {tier}")
    league_infos_list = []
    if tier in ["challengerleagues", "grandmasterleagues", "masterleagues"]:
        league_infos_list = [getTopLeagueEntries(queue=queue, tier=tier, region=region, RIOT_TOKENS = RIOT_TOKENS) for region in regions]
    else:
        if division is None:
            raise ValueError("Division must be specified for non-Challenger to Master divisions.")
        league_infos_list = [getNormalLeagueEntries(queue=queue, tier=tier, division=division, region=region, pages=pages, RIOT_TOKENS = RIOT_TOKENS) for region in regions]

    league_infos_list = list(zip(*league_infos_list))
    return pd.concat(league_infos_list[0],axis=0), pd.concat(league_infos_list[1],axis=0)
