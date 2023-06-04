from elasticsearch import Elasticsearch
import spacy
from spacy.training.example import Example
import random
import re


def find_person(_text):
    # Паттерн для поиска вхождения подстрок "Путин" или "Владимир Путин"
    pattern = r"(Путин|Путин[ауы]|Путин[(ом)]|Владимир Путин|Трамп|Трамп[ау]|Трамп[(ом)]|Дональд Трамп|Байден|Байден[" \
              r"ау]|Байден[(ом)]|Джо Байден|Пригожин|Пригожин[ау]|Пригожин[(ом)]|Евгений " \
              r"Пригожин|Зеленский|Зеленски[м]|Зеленского|Владимир Зеленский|Шойгу" \
              r"|Сергей Шойгу)"


    # Поиск вхождений с помощью регулярного выражения
    matches = re.finditer(pattern, _text, re.IGNORECASE)

    putins = []
    # Вывод результатов
    for match in matches:
        start = match.start()
        end = match.end()
        length = end - start
        putins.append((start, end-1, "PERSON"))
        # print(_text)
        print(
            f"{_text}\nНайдено вхождение: {match.group()} (Индексы: {start}-{end - 1}, Количество символов: {length})")
    return putins


es = Elasticsearch("http://localhost:9200")

index_name = "topwar"
request = {
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "title": {
              "query": "Пригожин Путин Шойгу Байден Зеленский Трамп",
              "operator": "or",
              "analyzer": "russian"
            }
          }
        },
        {
          "match": {
            "text": {
              "query": "Пригожин Путин Шойгу Байден Зеленский Трамп Пелоси",
              "operator": "or",
              "analyzer": "russian"
            }
          }
        }
      ]
    }
  }
}
response = es.search(index=index_name, size=10000, body=request)

hits = response['hits']['hits']

TRAIN_DATA = []

ml_file = open("./ML.txt", "w", encoding="UTF")
for hit in hits:
    document = hit['_source']
    # print(f"{document['title']}")
    title = document['title']
    coordinates = find_person(title)
    if len(coordinates) != 0:
        entities = {'entities': coordinates}
        ml_file.write(str((title, entities)) + '\n')
        TRAIN_DATA.append((title, entities))

    coordinates.clear()

    text = document['text']
    coordinates = find_person(text)
    if len(coordinates) != 0:
        entities = {'entities': coordinates}
        ml_file.write(str((text, entities)) + '\n')
        TRAIN_DATA.append((text, entities))
ml_file.close()

# Загрузка пустой модели для русского языка
nlp = spacy.blank("ru")

# Создание пайплайна для NER
ner = nlp.create_pipe("ner")
nlp.add_pipe("ner")

# Добавление меток именованных сущностей
ner.add_label("PERSON")

# Обучение модели
nlp.begin_training()
for i in range(10):  # Количество эпох обучения
    random.shuffle(TRAIN_DATA)
    losses = {}
    for text, annotations in TRAIN_DATA:
        doc = nlp.make_doc(text)
        example = Example.from_dict(doc, annotations)
        nlp.update([example], losses=losses)
    print("Loss:", losses)

nlp.to_disk("Persons_model")
