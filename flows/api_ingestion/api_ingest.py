from airflow.decorators import task
import requests
import json

import asyncio
from aiohttp import ClientSession

regional_base_uri = "https://sea.api.riotgames.com/"
local_base_uri = "https://ph2.api.riotgames.com/"

default_header = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com",
    "X-Riot-Token": "RGAPI-c99a91cf-db55-4b82-8ca3-0c30d66ce1ac",
}

# @task(task_id="api_request")
async def api_request(url, header) -> dict:
    async with ClientSession() as session:
        # asyncio.timeout(1200)
        # print(url)
        async with session.get(url, headers=header) as response:
            assert response.status == 200
            return await response.json()

# @task(task_id="multiple_matches_ingest")
async def matches_request(matches: list[str]) -> list[dict]:
    requests_list = []
    for match in matches:
        print("Fetching match: " + match)
        match_uri = regional_base_uri + "lol/match/v5/matches/" + match
        requests_list.append(api_request(match_uri, default_header))
    return await asyncio.gather(*requests_list)
    # print(responses)

async def league_entries_request(queue: str, tier :str, division:str, page:int = 1) -> list[dict]:
    request = api_request(f"{local_base_uri}lol/league-exp/v4/entries/{queue}/{tier}/{division}?page={page}", default_header)
    return await asyncio.gather(*[request])

async def top_league_entries_request(queue : str, tier:str):
    request = api_request(f"{local_base_uri}lol/league/v4/{tier}/by-queue/{queue}", default_header)
    return await asyncio.gather(*[request])

async def player_request(summonerID : str): #By SummonnerID -> "id" in JSON file
    request = api_request(f"{local_base_uri}lol/summoner/v4/summoners/{summonerID}", default_header)
    return await asyncio.gather(*[request])

# @task(task_id="multiple_matches_ingest")
def matches_ingest(matches: list[str]) -> list[dict]:
    return asyncio.run(matches_request(matches))

# @task(task_id="league_entries_ingest")
def league_entries_ingest(queue: str, tier :str, division:str, page:int = 1) -> list[dict]:
    return asyncio.run(league_entries_request(queue, tier, division, page))

# @task(task_id="top_league_entries_ingest")
def top_league_entries_ingest(queue: str, tier:str) -> list[dict]:
    return asyncio.run(top_league_entries_request(queue, tier))

# @task(task_id="player_ingest")
def player_ingest(summonerID : str) -> dict:
    return asyncio.run(player_request(summonerID))

# TEST FILES
# print(matches_ingest(["PH2_8088906"]))
# print(league_entries_ingest("RANKED_SOLO_5x5", "DIAMOND", "I", 1))
# print(top_league_entries_ingest("RANKED_SOLO_5x5", "grandmasterleagues"))
print(player_ingest("sD_MAmZPLmQ-GeyJbHgNdSY2YPJ43J9d316WbeSnf49MliIF2__5QbZXAg"))

# print(match_ingest("PH2_8088906")["metadata"])
