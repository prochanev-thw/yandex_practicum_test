import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    """
    extract data from sql-db
    :return:
    """

    # Мб с sqllite это и не такая большая проблема, но стоит стараться явно закрывать соединения
    # тут нам поможет contexlib.closing, т.к. обычный with sqlite3.connect("db.sqlite") as conn:
    # вернет нам только объект текущего соединения, но само соединение закрыто не будет
    # можно либо явно вызвать conn.close(), либо использовать contextlib.closing(), он будет сам дергать .close()
    # https://docs.python.org/3/library/contextlib.html#contextlib.closing
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    # РќР°РІРµСЂРЅСЏРєР° СЌС‚Рѕ РїРёР»РёС‚СЃСЏ РІ РѕРґРёРЅ sql - Р·Р°РїСЂРѕСЃ, РЅРѕ РјРЅРµ РєР°Рє-С‚Рѕ Р»РµРЅРёРІРѕ)
    # РџРѕР»СѓС‡Р°РµРј РІСЃРµ РїРѕР»СЏ РґР»СЏ РёРЅРґРµРєСЃР°, РєСЂРѕРјРµ СЃРїРёСЃРєР° Р°РєС‚РµСЂРѕРІ Рё СЃС†РµРЅР°СЂРёСЃС‚РѕРІ, РґР»СЏ РЅРёС… С‚РѕР»СЊРєРѕ id

    # тексты запросов лучше хранить в отдельных файлах с расширением SQL, в большинстве IDE будет гораздо удобнее работать в
    # отдельном файле
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),
        max(writer, writers)
        from movies
    """)

    # Хорошо бы использовать имя переменной, которая будет отражать суть того, что в ней лежит
    raw_data = cursor.fetchall()

    # не стоит хранит закоментированный код, он и так останется в коммитах

    # cursor.execute('pragma table_info(movies)')
    # pprint(cursor.fetchall())

    # РќСѓР¶РЅС‹ РґР»СЏ СЃРѕРѕС‚РІРµС‚СЃРІРёСЏ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂР° Рё С‡РµР»РѕРІРµРєРѕС‡РёС‚Р°РµРјРѕРіРѕ РЅР°Р·РІР°РЅРёСЏ

    # в Select можно явно вернуть 2 поля, которые мы потом используем
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


def transform(__actors, __writers, __raw_data):
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """

    # raw_data так и обозвать raw_movie_records, например, хотя бы чтобы там упоминалось movie
    documents_list = []
    for movie_info in __raw_data:
        # Р Р°Р·С‹РјРµРЅРѕРІР°РЅРёРµ СЃРїРёСЃРєР°

        # стоит попробовать dataclasses, читаемость кода увеличится.
        # https://habr.com/ru/companies/otus/articles/650257/ - статейка при них
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        # Эти операции удобнее было выполнить в SQL
        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            # вынести формирование этих списков отдельно и не понятно, что значит actor[1] и writer[1]
            # если не знать данные, то можно только предполагать, стоит вынести в переменные
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        for key in document.keys():
            if document[key] == 'N/A':
                # print('hehe')
                document[key] = None

        # как уже выше писал вынести document['actors'], document['actors'] в переменные
        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list

def load(acts):
    """

    :param acts:
    :return:
    """
    # Стоит вынести переменные в конфиг, тем более эти же значения используются в микросервисе на Flask
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    return True

if __name__ == '__main__':
    load(transform(*extract()))

# Рекомендации
# 1. extract стоит разбить на 3 отдельных метода. Или собрать всю информацию вместе через SQL и тогда можно обойтись одним
#
# 2. Часть кода где мы джоиним авторов с информацией из фильмов и сценаристов(writers, насколько понимаю) с инфомацией о фильмах лучше
# сделать через SQL.
#
# 3. Использовать аннотацию типов, хотя бы простых, int, str, bool, List[str]
# большинство IDE в этом случае дают отличные подсказки, также можно прогонять аннотированный код через анализаторы кода и находить ошибки
#
# 4. не хватает логов по процессу выполнения, и лучше вместо print использовать модуль logging для этих целей
#
# Полезные ссылки
# https://docs.python.org/3/library/contextlib.html#contextlib.closing - contextlib closing
# https://habr.com/ru/companies/otus/articles/650257/ - dataclasses
