[Your Product Name] is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games and all associated properties are trademarks or registered trademarks of Riot Games, Inc

# Reproduction Steps
## Obtaining your Riot API key
Riot API keys are needed to run this pipeline.  You can obtain your API key by following these steps:
1. Go to https://developer.riotgames.com/ and create an account. You can use your Riot account if you play League of Legends, Valorant, or any other Riot game.
2. Accept the terms of service and click on "Get Started" to create your API key.
3. Generate your API key and save it in `./credentials/credentials.json'
4. The more API keys, the merrier.  Note that the rate limits are 20 requests every 1 seconds(s) and 100 requests every 2 minutes(s). 
5. Note: You have to refresh the API every 24 hours if you have Production API (default).

## Deploying `all_flows` to Prefect
`cd {YOUR_PROJECT_DIR}`
`prefect deployment build ./flows/all_flows.py:etl_per_league --name="etl_all"`

(Optional) You may enter the following parameters in `etl_all-deployment.yaml` file:
```
parameters: {
    "queue": "RANKED_SOLO_5x5",
    "tier" : "challengerleagues",
    "division" : "I",
    "pages" : 1,
    "regions" : ["br1","eun1","euw1","jp1","kr","la1","la2","na1","oc1","tr1","ru","ph2","sg2","th2","tw2","vn2"],
    "ACCOUNT_INPUT_LIMIT" : 10000,
    "MATCH_INPUT_LIMIT" : 10000,
    "API_KEYS" : ["ENTER_YOUR_API_KEY_HERE remove this if you have api key set in credentials.json"], 
    "MAXIMUM_CONCURRENT_REQUESTS" : 500,
  }
```
Enter your API keys here OR in `./credentials/credentials.json' file.  The field is a list, so you can add more API keys if you want to hasten your API requests.  
Note, I HIGHLY suggest entering a field in ACCOUNT_ONPUT_LIMITS and MATCH_INPUT_LIMITS.  If you don't the program will query all accounts or matches that it receives from the API. /For context: A query from a league returns ~4000 accounts, and a match query from each account returns ~10 match by default./

`prefect deployment apply "etl_all-deployment.yaml"`

You should see this in your Prefect local UI
![](resources/images/2023-03-29-16-08-17.png)

Don't forget to add an agent: `prefect agent start default`