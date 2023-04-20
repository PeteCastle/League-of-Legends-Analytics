import asyncio
from dependencies.api_requests import getMatchesRequestFromApi, getMultiplePlayerMatchesFromApi, getLeagueEntriesFromApi, getTopLeagueEntriesFromApi, getMultiplePlayerEntriesFromApi, getMultiplePlayerMasteryIdsFromApi
import itertools
from prefect import task, flow
import random

@task(name="multiple_matches_ingest")
def ingestMultipleMatches(matches: list[str], RIOT_TOKENS: list, MAXIMUM_CONCURRENT_REQUESTS : int, REGION_GROUPINGS : dict, MATCH_INPUT_LIMIT) -> list[dict]:
    random.shuffle(matches)
    if MATCH_INPUT_LIMIT is not None:
        if len(matches) < MATCH_INPUT_LIMIT:
            MATCH_INPUT_LIMIT = len(matches)
        matches = list(itertools.islice(matches, 0, MATCH_INPUT_LIMIT))

    return asyncio.run(getMatchesRequestFromApi(matches,REGION_GROUPINGS,RIOT_TOKENS, MAXIMUM_CONCURRENT_REQUESTS))

@flow(name="ingestMultiplePlayerMatches")
def ingestMultiplePlayerMatches(player_uuids : list[tuple], MAXIMUM_CONCURRENT_REQUESTS : int,ACCOUNT_INPUT_LIMIT : int = None,  match_count : int = 20) -> list[dict]:
    print("Ingesting multiple player matches")
    random.shuffle(player_uuids)
    if ACCOUNT_INPUT_LIMIT is not None:
        if len(player_uuids) < ACCOUNT_INPUT_LIMIT:
            ACCOUNT_INPUT_LIMIT = len(player_uuids)
        player_uuids = list(itertools.islice(player_uuids, 0, ACCOUNT_INPUT_LIMIT))
    return asyncio.run(getMultiplePlayerMatchesFromApi(player_uuids, match_count,MAXIMUM_CONCURRENT_REQUESTS))
    
@task(name="ingestLeagueEntries")
def ingestLeagueEntries(queue: str, tier :str, division:str, region, riot_token : str, page:int = 1) -> list[dict]:
    return asyncio.run(getLeagueEntriesFromApi(queue, tier, division, page, region, riot_token))[0]

@task(name="ingestTopLeagueEntries")
def ingestTopLeagueEntries(queue: str, tier:str,region, riot_token : str) -> dict:
    return asyncio.run(getTopLeagueEntriesFromApi(queue, tier, region,riot_token))[0]

@flow(name="ingestMultiplePlayerEntries")
def ingestMultiplePlayerEntries(players : list[tuple], ACCOUNT_INPUT_LIMIT: int = None, MAXIMUM_CONCURRENT_REQUESTS : int= None) -> list[dict]:
    print("Ingesting multiple player entries")
    random.shuffle(players)
    if ACCOUNT_INPUT_LIMIT is not None:
        if len(players) < ACCOUNT_INPUT_LIMIT:
            ACCOUNT_INPUT_LIMIT = len(players)
        players = list(itertools.islice(players, 0, ACCOUNT_INPUT_LIMIT))

    return asyncio.run(getMultiplePlayerEntriesFromApi(players,MAXIMUM_CONCURRENT_REQUESTS))

@flow(name="ingestMultiplePlayerMasteryIds")
def ingestMultiplePlayerMasteryIds(summoner_ids : list[tuple], MAXIMUM_CONCURRENT_REQUESTS : int= None, ACCOUNT_INPUT_LIMIT: int= None) -> list[dict]:
    random.shuffle(summoner_ids)
    if ACCOUNT_INPUT_LIMIT is not None:
        if len(summoner_ids) < ACCOUNT_INPUT_LIMIT:
            ACCOUNT_INPUT_LIMIT = len(summoner_ids)
    summoner_ids = list(itertools.islice(summoner_ids, 0, ACCOUNT_INPUT_LIMIT))
    
    return asyncio.run(getMultiplePlayerMasteryIdsFromApi(summoner_ids,MAXIMUM_CONCURRENT_REQUESTS))