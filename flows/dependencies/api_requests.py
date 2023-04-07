from aiohttp import ClientSession
from aiohttp import ClientConnectionError
import asyncio
from datetime import datetime
import numpy as np
# FOR DEBUG ONLY:

# count = 0
# rateLimitedCount = 0
# start_date = datetime.now()

# async def getApiRequest(url, riot_token: str, fromRateLimited = 0) -> dict:
#     header = {
#         "Accept-Language": "en-US,en;q=0.9",
#         "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
#         "Origin": "https://developer.riotgames.com",
#         "X-Riot-Token": riot_token,
#     }
#     async with ClientSession() as session:
#         async with session.get(url, headers=header) as response:
#             global rateLimitedCount
#             global count 
#             count+=1
#             if fromRateLimited :
#                 rateLimitedCount -=1
#                 print(f"No longer Rate limited count: {rateLimitedCount}")
#             print(f"Took {datetime.now() - start_date} to complete {count} requests on {url} and API token {riot_token}")
            
#             if response.status == 429:
#                 retry_after = int(response.headers.get('Retry-After', '1'))
#                 print("Rate limited, retrying after " + str(retry_after) + " seconds.")

#                 rateLimitedCount +=1
#                 print("Rate limited count: " + str(rateLimitedCount))

#                 await asyncio.sleep(retry_after)
#                 return await getApiRequest(url, riot_token,1)
#             if response.status != 200:
#                 print(f"Error {response.status} on {url} with API {riot_token}")
#             assert response.status == 200
#             return await response.json()
        
async def getApiRequest(url, riot_token: str) -> dict:
    header = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": riot_token,
    }
    async with ClientSession() as session:
        try:
            async with session.get(url, headers=header) as response:
                
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', '1'))
                    # print("Rate limited, retrying after " + str(retry_after) + " seconds.")

                    await asyncio.sleep(retry_after)
                    return await getApiRequest(url, riot_token)
                elif response.status == 503:
                    # retry_after = int(response.headers.get('Retry-After', '1'))
                    print(f"503 Service Unavailable Error on {url} with API {riot_token}.  Retrying in 10 seconds...")
                    await asyncio.sleep(10)
                    return await getApiRequest(url, riot_token)

                # print(f"Completed requests on {url} and API token {riot_token}")

                assert response.status == 200, f"Error {response.status} on {url} with API {riot_token}"
                return await response.json()
        except ClientConnectionError as e:
            print(f"Connection Error: {e}.  Retrying...")
            return await getApiRequest(url, riot_token)



        
async def getMatchesRequestFromApi(matches: list[str], REGION_GROUPINGS: dict, RIOT_TOKENS: list, MAXIMUM_CONCURRENT_REQUESTS : int) -> list[dict]:
    requests_list = []
    for match in matches:
        region = REGION_GROUPINGS[match.split('_')[0].lower()]
        match_uri = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match}"
        requests_list.append(getApiRequest(match_uri, np.random.choice(RIOT_TOKENS,1)[0]))

    semaphore = asyncio.Semaphore(MAXIMUM_CONCURRENT_REQUESTS)
    async def semaphored_request(request):
        async with semaphore:
            return await request
    return await asyncio.gather(*(semaphored_request(request) for request in requests_list))

async def getMultiplePlayerMatchesFromApi(player_uuids : list[tuple], match_count : int, MAXIMUM_CONCURRENT_REQUESTS : int) -> list[dict]:
    requests_list = []
    for puuid, region_group, riot_token in player_uuids:
        request = getApiRequest(f"https://{region_group}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={match_count}", riot_token)
        requests_list.append(request)

    semaphore = asyncio.Semaphore(MAXIMUM_CONCURRENT_REQUESTS)
    async def semaphored_request(request):
        async with semaphore:
            return await request
        
    return await asyncio.gather(*(semaphored_request(request) for request in requests_list))

    
async def getLeagueEntriesFromApi(queue: str, tier :str, division:str, page:int , region : str, riot_token : str) -> list[dict]:
    request = getApiRequest(f"https://{region}.api.riotgames.com/lol/league-exp/v4/entries/{queue}/{tier}/{division}?page={page}", riot_token)
    return await asyncio.gather(*[request])

async def getTopLeagueEntriesFromApi(queue : str, tier:str, region : str, riot_token : str) -> list[dict]:
    request = getApiRequest(f"https://{region}.api.riotgames.com/lol/league/v4/{tier}/by-queue/{queue}", riot_token)
    return await asyncio.gather(*[request])

async def getMultiplePlayerEntriesFromApi(players : list[tuple], MAXIMUM_CONCURRENT_REQUESTS : int): #By SummonnerID -> "id" in JSON file
    requests_list = []

    for region, summonerID, riot_token in players:
        requests_list.append(getApiRequest(f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerID}", str(riot_token)))
    
    semaphore = asyncio.Semaphore(MAXIMUM_CONCURRENT_REQUESTS)
    async def semaphored_request(request):
        async with semaphore:
            return await request
    
    # return await asyncio.gather(*requests_list)
    return await asyncio.gather(*(semaphored_request(request) for request in requests_list))

async def getMultiplePlayerMasteryIdsFromApi(summoner_ids : list[tuple], MAXIMUM_CONCURRENT_REQUESTS : int):
    requests_list = []
    for encryptedSummonerId, region, riot_token in summoner_ids:
        request = getApiRequest(f"https://{region}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-summoner/{encryptedSummonerId}", riot_token)
        requests_list.append(request)

    semaphore = asyncio.Semaphore(MAXIMUM_CONCURRENT_REQUESTS)
    async def semaphored_request(request):
        async with semaphore:
            return await request
        
    return await asyncio.gather(*(semaphored_request(request) for request in requests_list))