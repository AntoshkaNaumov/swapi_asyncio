import asyncio
import re
import aiohttp

from more_itertools import chunked
from datetime import datetime
from cache import AsyncLRU
from db import engine, Session, People, Base


CHUNK_SIZE = 10


async def get_person(people_id: int, session):
    """функция отправки запроса к api"""
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        data = await response.json()
        return people_id, response.status, data


async def get_result(id_range, session):
    """функция-генератор выгрузки персонажей по 10 человек (CHUNK_SIZE)"""
    for id_chunk in chunked(id_range, CHUNK_SIZE):
        tasks = (asyncio.create_task(get_person(i, session)) for i in id_chunk)
        count_status_404 = 0
        for task in tasks:
            result = await task
            if result[1] == 404: count_status_404 += 1
            yield result
        if count_status_404 == CHUNK_SIZE: break  # выход если весь чанк пустой #raise StopIteration


@AsyncLRU(maxsize=100)
async def get_additional_data(url, id_add, key, session):
    """функция отправки запроса к api"""
    async with session.get(f'https://swapi.dev/api/{url}/{id_add}') as response:
        data = await response.json()
        return data[key]


async def get_data_by_ids(url, ids, key, session):
    """функция-генератор"""
    tasks = (asyncio.create_task(get_additional_data(url, i, key, session)) for i in ids)

    for task in tasks:
        yield await task


def get_identifiers_from_urls(*urls):
    ids = []

    for url in urls:
        result = re.search(r'\d+', url).group()
        ids.append(result)
    return ids


async def create_str_for_plus_data(urls, url, key, session):
    ids = get_identifiers_from_urls(*urls)
    result_list = []
    async for film in get_data_by_ids(url, ids, key, session):
        result_list.append(film)
    res_string = ', '.join(result_list)
    return res_string


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all) # запускаем миграцию
        await conn.commit()

    async with aiohttp.ClientSession() as session_http:
        async with Session() as session_base:

            async for person in get_result(range(1, 201), session_http):
                print(person)
                if person[1] == 200:
                    person_json = person[2]
                    films_string = await create_str_for_plus_data(person_json['films'], 'films', 'title',
                                                                           session_http)
                    homeworld_string = await create_str_for_plus_data([person_json['homeworld']], 'planets',
                                                                               'name', session_http)
                    species_string = await create_str_for_plus_data(person_json['species'], 'species', 'name',
                                                                             session_http)
                    starships_string = await create_str_for_plus_data(person_json['starships'], 'starships',
                                                                               'name', session_http)
                    vehicles_string = await create_str_for_plus_data(person_json['vehicles'], 'vehicles',
                                                                              'name', session_http)

                    session_base.add(People(
                        id=person[0],
                        birth_year=person_json['birth_year'],
                        eye_color=person_json['eye_color'],
                        films=films_string,
                        gender=person_json['gender'],
                        hair_color=person_json['hair_color'],
                        height=person_json['height'],
                        homeworld=homeworld_string,
                        mass=person_json['mass'],
                        name=person_json['name'],
                        skin_color=person_json['skin_color'],
                        species=species_string,
                        starships=starships_string,
                        vehicles=vehicles_string,

                    ))

                    await session_base.commit() # загружаем в базу данных



start = datetime.now()
asyncio.run(main())
print('Время выполнения', datetime.now() - start)
