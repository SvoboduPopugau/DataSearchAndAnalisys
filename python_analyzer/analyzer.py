from elasticsearch import Elasticsearch
from textblob import TextBlob
from googletrans import Translator
import spacy

MAPPING = {
                "properties": {
                  "datetime": {
                    "type": "date"
                  },
                  "id": {
                    "type": "keyword"
                  },
                  "text": {
                    "type": "text",
                    "analyzer": "russian"
                  },
                  "title": {
                    "type": "text",
                    "analyzer": "russian"
                  },
                  "url": {
                    "type": "keyword"
                  },
                  "persons": {
                    "type": "keyword"
                  },
                  "sentiment": {
                    "type": "keyword"
                  }
                }
            }


def db_add(doc, _person, label):
    # translator = Translator()
    # translation = translator.translate(_person, dest='en').text
    _person = str(_person).replace(':', '')
    _person = str(_person).replace('-', '')
    _person = str(_person).replace(' ', '_').lower()
    index = 'personalised_topwar'

    if not es.indices.exists(index=index):
        es.indices.create(index=index, body={"mappings": MAPPING})
        # es.indices.put_mapping(index=index, body=MAPPING)
        print(f"Индекс {index} успешно создан.")
    # else:
    #     print(f"Индекс {index} уже существует.")

    if not es.exists(index=index, id=doc['id']):
        doc['persons'] = persons
        doc['sentimental'] = label
        es.index(index=index, body=doc, id=doc['id'])
        print(f"Документ: {doc['id']} успешно создан")
    else:
        print(f"Документ: {doc['id']} уже существует")


# Устанавливаем подключение к Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Параметры запроса
index_name = "topwar"
page_size = 1000  # Размер страницы (количество документов на странице)
total_documents = 0

# Переменные для пагинации
current_page = 1
scroll_id = None
res_file = open("./topwar_docs.txt", 'w', encoding='UTF')

# Выполняем запросы с пагинацией, пока не получим все документы
while True:
    # Выполняем первый запрос или запрос с использованием scroll_id
    if scroll_id is None:
        response = es.search(
            index=index_name,
            scroll="5m",  # Время жизни scroll контекста
            size=page_size,
            body={"query": {"match_all": {}}}
        )
    else:
        response = es.scroll(scroll_id=scroll_id, scroll="5m")

    # Получаем результаты и обновляем переменные
    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]
    total_documents += len(hits)
    print(f"Количество полученных документов: {total_documents}")

    nlp = spacy.load("ru_core_news_sm")

    for hit in hits:
        document = hit['_source']
        # Создаем объект TextBlob для текста
        blob = TextBlob(document['text'])

        # Определяем эмоциональную окраску текста
        blob_sentiment = blob.sentiment.polarity

        # Определяем метку эмоциональной окраски
        if blob_sentiment > 0:
            blob_label = "Положительная"
        elif blob_sentiment < 0:
            blob_label = "Отрицательная"
        else:
            blob_label = "Нейтральная"

        res_file.write(f"{str(document)}\n blob label: {blob_label} - {blob_sentiment}\n")

        # Обработка текста с помощью модели spaCy
        doc = nlp(document["title"])

        persons = []
        for ent in doc.ents:
            if ent.label_ == "PER":
                persons.append(ent.text)

        db_add(document, persons, blob_label)

        # if len(persons) > 0:
            # print(document['title'])
            # print("Именованные сущности персон: ", end='')
            # for person in persons:
                # db_add(document, person)
                # print(person, end=' ')
            # print()

    # Проверяем условие завершения пагинации
    if len(hits) < page_size:
        break

# Очищаем scroll контекст
es.clear_scroll(scroll_id=scroll_id)

# Выводим результаты
print(f"Количество документов в индексе '{index_name}': {total_documents}")

res_file.close()

