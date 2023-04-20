from datetime import datetime
from prefect import flow
import pandas as pd
import itertools
from typing import Tuple

from dependencies.method_ingestion import ingestMultipleMatches

# @flow(name="getMatchGeneralInfo")
def getMatchGeneralInfo(raw_match: dict) -> dict:
    match = {}

    match['matchId'] = raw_match['metadata']['matchId']
    match['last_update'] = datetime.now()
    keys_to_exlude = {
        "participants",
        "teams",
    }
    match |= { key:raw_match["metadata"][key] for key in set(set(raw_match["metadata"].keys()) - keys_to_exlude)}
    match |= { key:raw_match["info"][key] for key in set(set(raw_match["info"].keys()) - keys_to_exlude)}
    return match

# @flow(name = "getMatchPlayersInfo")
def getMatchPlayersInfo(raw_match: dict) -> list[dict]:
    if raw_match is None:
        print("Match none detected")
        return {}
    
    keys_initial_exclude = {
        "challenges",
        "perks",
    }

    keys_exclude = {
        "challenges"
    }

    raw_participants_details  : list[dict] = raw_match["info"]["participants"]

    final_participants_details : list[dict] = []
    
    for raw_participant in raw_participants_details:

        participant = {}
        participant["matchId"] = raw_match["metadata"]["matchId"]
        participant['last_update'] = datetime.now()

        #Normal Keys
        participant |= { key:raw_participant[key] for key in set(set(raw_participant.keys()) - keys_initial_exclude - keys_exclude)}

        #Challenges DTO flattened -> Removed and separated to another table
        # if "challenges" in raw_participant.keys():
        #     participant |= { f"challenges_{key}":raw_participant["challenges"][key] for key in raw_participant["challenges"].keys()}

        #Runes DTO stat perks flattened
        participant |= { f"statPerks_{key}": raw_participant["perks"]["statPerks"][key] for key in raw_participant["perks"]["statPerks"].keys() }
        
        #Runes DTO styles flattened
        for style in raw_participant["perks"]["styles"]:
            temp = {}

            style_type = style['description']
            temp[f"{ style_type }_id"] = style["style"]
            
            #PerkStyleSelectionDto
            for index, perk in enumerate(style["selections"]):
                temp[f"{style_type}_{index}_id"] = perk["perk"]
                temp[f"{style_type}_{index}_var1"] = perk["var1"]
                temp[f"{style_type}_{index}_var2"] = perk["var2"]
                temp[f"{style_type}_{index}_var3"] = perk["var3"]
            
            participant |= temp

        final_participants_details.append(participant)
    
    return final_participants_details

# @flow(name = "getMatchTeamsInfo")
def getMatchTeamsInfo(raw_match: dict) -> dict:
    teams_info_raw = raw_match["info"]["teams"]
    teams_info = {}

    teams_info["matchId"] = raw_match["metadata"]["matchId"]
    teams_info['last_update'] = datetime.now()
    for team in teams_info_raw:
        team_name = "blue" if team["teamId"] == 100 else "red"
        for bans in team["bans"]:
            teams_info[f"bans_turn_{bans['pickTurn']}"] = bans["championId"]
        for objective in team["objectives"].keys():
            teams_info[f"{team_name}_{objective}_isFirstKill"] = team["objectives"][objective]["first"]
            teams_info[f"{team_name}_{objective}_kills"] = team["objectives"][objective]["kills"]
        teams_info[f"{team_name}_win"] = team["win"]
    
    return teams_info

def getMatchPlayerChallengesInfo(raw_match:dict) -> list[dict]:
    players_challenges_info = []
    for participant in raw_match["info"]["participants"]:
        if "challenges" not in participant.keys():
            continue
        player_challenges_info = {}
        player_challenges_info["summonerName"] = participant["summonerName"]
        player_challenges_info["matchId"] = raw_match["metadata"]["matchId"]
        player_challenges_info['last_update'] = datetime.now()
        player_challenges_info |=  participant.get("challenges")
        players_challenges_info.append(player_challenges_info)

    return players_challenges_info

@flow(name = "getMultipleMatchInfo")
def getMultipleMatchInfo(match_history_raw: list, RIOT_TOKENS : list, MAXIMUM_CONCURRENT_REQUESTS : int,  REGION_GROUPINGS : dict, MATCH_INPUT_LIMIT : int = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    match_history_list = list(itertools.chain.from_iterable(match_history_raw))
    raw_matches = ingestMultipleMatches(match_history_list, RIOT_TOKENS, MAXIMUM_CONCURRENT_REQUESTS,REGION_GROUPINGS, MATCH_INPUT_LIMIT )

    match_general_info = []
    match_players_info = []
    match_teams_info = []
    match_players_challenges_info = []

    for match in raw_matches:
        match_general_info.append(getMatchGeneralInfo(match))
        match_players_info.append(getMatchPlayersInfo(match))
        match_teams_info.append(getMatchTeamsInfo(match))
        match_players_challenges_info.append(getMatchPlayerChallengesInfo(match))

    match_general_info = pd.DataFrame(match_general_info)
    match_players_info = pd.DataFrame(itertools.chain.from_iterable(match_players_info))
    match_teams_info = pd.DataFrame(match_teams_info)
    match_players_challenges_info = pd.DataFrame(itertools.chain.from_iterable(match_players_challenges_info))
    
    return match_general_info, match_players_info, match_teams_info, match_players_challenges_info


