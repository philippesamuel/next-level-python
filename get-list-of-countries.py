import asyncio
import functools
from functools import partial
from pprint import pprint
from time import perf_counter
from typing import TypedDict, Iterable

import requests
from pydantic import BaseModel

API_ROOT = 'http://geodb-free-service.wirefreethought.com/v1'
NUMBER_OF_COUNTRIES = 198  # specified by geodb-cities service
CHUNK_SIZE = 1
N = 10

async def main() -> None:
    get_countries_limited = partial(get_countries, limit=CHUNK_SIZE)
    get_countries_limited_sync = partial(get_countries_sync, limit=CHUNK_SIZE)
    
    # sync
    time_before = perf_counter()
    offsets = range(0, N, CHUNK_SIZE)
    
    for offset in offsets:
        countries = get_countries_limited_sync(offset)
        print([country.name for country in countries])
    
    print(f"Total time (sync): {perf_counter() - time_before}")

    # async
    time_before = perf_counter()
    offsets = range(0, N, CHUNK_SIZE)
    responses = await asyncio.gather(*[get_countries_limited(offset) for offset in offsets])

    for countries in responses:
        print([country.name for country in countries])

    print(f"Total time (async): {perf_counter() - time_before}")


class CountryResponse(BaseModel):
    code: str
    currencyCodes: list[str]
    name: str
    wikiDataId: str  


async def get_countries(offset: int, limit: int) -> Iterable[CountryResponse]:
    response = await get_geodb_cities_data(
            endpoint="geo/countries", 
            query_params={"offset": offset, "limit": limit}
            )
    countries = response["data"]
    return (CountryResponse(**country) for country in countries)


def get_countries_sync(offset: int, limit: int) -> Iterable[CountryResponse]:
    response = requests.get(f"{API_ROOT}/geo/countries?offset={offset}&limit={limit}")
    countries = response.json()["data"]
    return (CountryResponse(**country) for country in countries)


async def get_geodb_cities_data(endpoint: str, query_params: dict[str, str] | None = None):
    query_str = ""
    if query_params:
        query_elements = (f"{k}={v}" for k,v in query_params.items())
        query_str = "&".join(query_elements)
    return await http_get(f"{API_ROOT}/{endpoint}?{query_str}")


JSON = int | str | float | bool | None | dict[str, "JSON"] | list["JSON"]
JSONObject = dict[str, JSON]
JSONList = list[JSON]


# @functools.lru_cache
def http_get_sync(url: str) -> JSONObject:
    response = requests.get(url)
    return response.json()


async def http_get(url: str) -> JSONObject:
    return await asyncio.to_thread(http_get_sync, url)


if __name__ == "__main__":
    asyncio.run(main())

