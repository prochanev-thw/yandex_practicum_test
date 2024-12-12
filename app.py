from flask import Flask, abort, request, jsonify
import elasticsearch as ES

from validate import validate_args

app = Flask(__name__)


@app.route('/')
def index():
    return 'worked'

@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)

    # Кажется 400 даже лучше подойдет,
    # Но круто, что этот нюанс учтен
    # 422 вроде не совсем подходит
    # https://developer.mozilla.org/ru/docs/Web/HTTP/Status/422
    if not validate['success']:
        return abort(422)

    # В идеале завернуть работу с эластик серч в отдельный клиент, который будет принимать
    # ES клиент и всю логику по обработке запросов делать внутри себя
    # сами эндпоинты должны быть как можно "тоньше"
    # В предельном идеальном случае эндпоинты служат для передачи информации о запросе во внутринние функции и клссы приложения,
    # Эти функции и классы не должны знать о сущностях Flask, а принимать уже простые типы, ну или кастомные типы
    # Которые мы сами разработаем в более сложных случаях
    # Есть классически паттерн MVC(Model - View - Controller) - https://doka.guide/tools/architecture-mvc/
    # В нашем случае Model - это класс, который будет отвечать за запросы к ES
    # View - т.к. у нас нету отображения, то это просто как вы вернем ответ пользователю, в каком формате
    # Controlle - это часть, где мы описываем эндпоинт Flask приложнеия, по паттерну MVC Controller отвечает за
    # обработку запросов, передаче их модели, обработка сетевых ошибок, эдакий слой доставки данных до модели.
    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # РўСѓС‚ СѓР¶Рµ РІР°Р»РёРґРЅРѕ РІСЃРµ
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # РЈС…РѕРґРёС‚ РІ С‚РµР»Рѕ Р·Р°РїСЂРѕСЃР°. Р•СЃР»Рё Р·Р°РїСЂРѕСЃ РЅРµ РїСѓСЃС‚РѕР№ - РјСѓР»СЊС‚РёСЃРµСЂС‡, РµСЃР»Рё РїСѓСЃС‚РѕР№ - РІС‹РґР°РµС‚ РІСЃРµ С„РёР»СЊРјС‹
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    # стоит выделить ответ в переменную, чтобы было четко понятно, что мы возвращаем
    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):

    # 1. Такие переменные как хост, порт и в особенности секреты(ключи, явки, пароли)
    # лучше хранить в файле с конфигом, возможно определять их в переменных окружения
    # Лучше воспользоваться возможность Flask читать конфиги из внешних файлов
    # из плюсов этого подхода можно сделать конфиг для dev/prod/test и при инициализации приложения выбирать нужный
    # в зависимости от переданных параметров
    # Вот хорошая статья на эту тему
    # https://hackersandslackers.com/configure-flask-applications/#:~:text=Configuring%20Flask%20From%20Class%20Objects
    # 2. При инициализации всяких баз данных можно укзаать, таймаут, который мы готовы ожидать
    # его также нужно указать в конфиге
    # понятно, что от ES ожидается достаточно быстрый ответ, но мы должны учитывать этот момент
    # например инициализация класса подключения могла бы выглядеть так
    #
    # es_client = ES.Elasticsearch(
    #                   timeout=app.config['ES_TIMEOUT'],
    #                   max_retries=app.config['ES_MAX_RETRIES'],
    #                   retry_on_timeout=app.config['ES_RETRY_ON_TIMEOUT']
    #             )


    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    # тут лучше вернуть код 500
    # вот примеры кодов, которые нужно возвращать в той или иной ситуации
    # https://developer.mozilla.org/ru/docs/Web/HTTP/Status
    # лучше придерживаться именно такого подхода в обработке серверных ошибок, тогда фронт, который схватит ошибку
    # или еще какой-нибудь фронтовый клиент не будут удивляться странному ответу от сервера
    if not es_client.ping():
        print('oh(')

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    # тут можно воспользоваться менеджером контекста, обычно этот API поддерживается большинством библиотек,
    # которые делают какую-то IO работу (IO - Input/Output - нагрузка связанная с чтением/записью в файлы, сеть, консоль)
    # и в некоторых случаях можно забыть закрыть соединения или почистить какие-то ресурсы, которые потом могут сожрать память.
    # хорошая статейка с Хабр
    # https://habr.com/ru/companies/auriga/articles/724030/
    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)

# Рекомендации
# 1. Явно выделять клиентский код, который "отвязан" от Flask, и в эндпоинтах только дергаются его функции и методы, почитать про MVC(Model View Controller)
#
# 2. Часть кода где мы джоиним авторов с информацией из фильмов и сценаристов(writers, насколько понимаю) с инфомацией о фильмах лучше
# сделать через SQL.
#
# 3. Не хватает обработки ошибок, причем в коде эндпоинта нужно обрабатывать ошибки связанные с обработкой request/response,
# а в коде смого приложения, например ES клиент, наш, кастомный уже обрабатывать ошибки связанные с ES.
#
# 4. не хватает логов по процессу выполнения, и лучше вместо print использовать модуль logging для этих целей
#
# Полезные ссылки
# https://developer.mozilla.org/ru/docs/Web/HTTP/Status/422 - статусы ответов серверов
# https://doka.guide/tools/architecture-mvc/ - model - view - controller паттерн
# https://hackersandslackers.com/configure-flask-applications/#:~:text=Configuring%20Flask%20From%20Class%20Objects - хранение конфигов в отдельном файле
# https://habr.com/ru/companies/auriga/articles/724030/ - Context manager в рамках языка Python
