#!/usr/bin/env python3
import sys
import os
import requests
import itertools  
import json
from ratelimit import limits, sleep_and_retry
import queue as Queue
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv
from neo4j import GraphDatabase, basic_auth
from src import test
from src import Digimon

load_dotenv(find_dotenv())
dot_env = os.environ

"""
Module Docstring
"""

__author__ = "Tiago Loriato"
__version__ = "0.1.0"
__license__ = "GNU GPLv3"

class Storage():
    def __init__(self, path):
        self.__path = path
        return

    def __load(self):
        for file in os.listdir(self.__path):
            return
        return

    def retrieve(self, name):
        if (os.path.isfile(f'{self.__path}/{name}.html')):
            with open(f'{self.__path}/{name}.html') as html_file:
                content = html_file.read()
            return content
        return None

    def add_page(self, name, html):
        open(f'{self.__path}/{name}.html', 'wb').write(html)
        return

class Database():
    #TODO: Optimize the hell out of this later
    def __init__(self):
        self.session = self.__database_session()

    def __database_session(self):
        driver = GraphDatabase.driver(dot_env["NEO4J_BOLT_IP"], auth=basic_auth(dot_env["NEO4J_USER"], dot_env["NEO4J_PASSWORD"]))
        return driver.session()
    
    def __get_digimon(self, digimon_name):
        query = "MATCH (n:Digimon) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, {"name": digimon_name}).single()
        
    def __create_by_name(self, digimon_name):
        return self.session.run("CREATE (d:Digimon {name: $name}) RETURN d.name", name=digimon_name).single()
    
    def __get_or_create_digimon_by_name(self, digimon_name):
        node = self.__get_digimon(digimon_name)
        if node is None:
            print(f'Criando {digimon_name}')
            return self.__create_by_name(digimon_name)
        return node
    
    def __get_attribute(self, attribute_name):
        query = "MATCH (n:Attribute) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, {"name": attribute_name}).single()

    def __create_attribute(self, attribute_name):
        return self.session.run("CREATE (d:Attribute {name: $name}) RETURN d.name", name=attribute_name).single()

    def __get_or_create_attribute_by_name(self, attribute_name):
        attribute = self.__get_attribute(attribute_name)
        if attribute is None:
            print(f'Creating attribute {attribute_name}')
            return self.__create_attribute(attribute_name)
        return attribute

    def __get_family(self, family_name):
        query = "MATCH (n:Family) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, {"name": family_name}).single()

    def __create_family(self, family_name):
        return self.session.run("CREATE (d:Family {name: $name}) RETURN d.name", name=family_name).single()

    def __get_or_create_family_by_name(self, family_name):
        family = self.__get_family(family_name)
        if family is None:
            print(f'Creating family {family_name}')
            return self.__create_family(family_name)
        return family

    def __add_evolution(self, digimon_node, evolution_node):
        digimon_name = digimon_node[0]
        evolution_name = evolution_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon_name MATCH (d:Digimon) WHERE d.name = $evolution_name MERGE (n)-[:EVOLVES_TO]->(d)", digimon_name=digimon_name, evolution_name=evolution_name)

    def __add_variation(self, digimon_node, variation_node):
        digimon_name = digimon_node[0]
        variation_name = variation_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon_name MATCH (d:Digimon) WHERE d.name = $variation_name MERGE (n)-[:IS_VARIATION]->(d)", digimon_name=digimon_name, variation_name=variation_name)
    
    def __add_attribute(self, digimon_node, attribute_node):
        digimon = digimon_node[0]
        attribute = attribute_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon MATCH (a:Attribute) WHERE a.name = $attribute MERGE (n)-[:HAS_ATTRIBUTE]->(a)", digimon=digimon, attribute=attribute)

    def __add_family(self, digimon_node, family_node):
        digimon = digimon_node[0]
        family = family_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon MATCH (f:Family) WHERE f.name = $family MERGE (n)-[:BELONGS_TO]->(f)", digimon=digimon, family=family)

    def __get_type(self, type_name):
        query = "MATCH (n:Type) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, {"name": type_name}).single()

    def __create_type(self, type_name):
        return self.session.run("CREATE (d:Type {name: $name}) RETURN d.name", name=type_name).single()

    def __get_or_create_type_by_name(self, type_name):
        type_name_node = self.__get_type(type_name)
        if type_name_node is None:
            print(f'Creating type {type_name}')
            return self.__create_type(type_name)
        return type_name_node

    def __add_type(self, digimon_node, type_node):
        digimon = digimon_node[0]
        type_name_node = type_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon MATCH (f:Type) WHERE f.name = $type MERGE (n)-[:BELONGS_TO]->(f)", digimon=digimon, type=type_name_node)

    def __get_level(self, level_name):
        query = "MATCH (n:Level) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, {"name": level_name}).single()

    def __create_level(self, level_name):
        return self.session.run("CREATE (d:Level {name: $name}) RETURN d.name", name=level_name).single()

    def __get_or_create_level_by_name(self, level_name):
        level_name_node = self.__get_level(level_name)
        if level_name_node is None:
            print(f'Creating level {level_name}')
            return self.__create_level(level_name)
        return level_name_node

    def __add_level(self, digimon_node, level_node):
        digimon = digimon_node[0]
        level_name_node = level_node[0]
        return self.session.run("MATCH (n:Digimon) WHERE n.name = $digimon MATCH (f:Level) WHERE f.name = $level MERGE (n)-[:BELONGS_TO]->(f)", digimon=digimon, level=level_name_node)

    def add(self, digimon):
        # TODO: Deal with EN vs JP versions

        main_node = self.__get_or_create_digimon_by_name(digimon.name)

        if digimon.level is not None:
            level_node = self.__get_or_create_level_by_name(digimon.level)
            self.__add_level(main_node, level_node)

        for type_node in digimon.type:
            type_node = self.__get_or_create_type_by_name(type_node)
            self.__add_type(main_node, type_node)

        for evolution in digimon.next_forms:
            next_form = self.__get_or_create_digimon_by_name(evolution)
            self.__add_evolution(main_node, next_form)

        for attribute in digimon.attribute:
            attribute_name = self.__get_or_create_attribute_by_name(attribute)
            self.__add_attribute(main_node, attribute_name)
        
        for family in digimon.family:
            family_name = self.__get_or_create_family_by_name(family)
            self.__add_family(main_node, family_name)
        
        for children in digimon.prior_forms:
            children = self.__get_or_create_digimon_by_name(children)
            self.__add_evolution(children, main_node)
        # 
        for variation in digimon.variations:
            other = self.__get_or_create_digimon_by_name(variation)
            self.__add_variation(main_node, other)

class DummyQueue():
    def __init__(self, storage=None):
        self.__Storage = storage
        self.__cache = {}
        self.__queue = []

    @sleep_and_retry
    @limits(calls=600000000000, period=60*60)
    def __crawler(self, name):

        if (self.__Storage is not None):
            retrieval = self.__Storage.retrieve(name)

            if (retrieval is not None):
                return retrieval

        print(f'Getting {name} from Wiki...')
        base_url = "https://digimon.fandom.com/wiki/"
        response = requests.get(base_url + name, headers={'Accept-Encoding': 'identity'})
        
        if response.status_code != 200:
            print(response.status_code)
            return None
            
        else:
            if (self.__Storage is not None):
                self.__Storage.add_page(name, response.content)
            return response.content

    def add(self, name):
        if (self.__cache.get(name, None) is None):
            self.__cache[name] = 1
            self.__queue.append(name)
            return True
        else:
            return False
    
    def get(self):
        digimon = self.__queue.pop()
        if digimon is not None:
            content = self.__crawler(digimon)
            if (content is None):
                return self.get()
            return content
        return None
        
def main():
    """ Main entry point of the app """
    store = Storage("cache")
    queue = DummyQueue(storage=store)
    db = Database()

    # Loads the Cache into the Database
    

    queue.add('Botamon')

    htmldoc = queue.get()

    while(htmldoc is not None):
        digimon = Digimon(htmldoc, queue.add, db.add)
        htmldoc = queue.get()

if __name__ == "__main__":
    """ This is executed when run from the command line """

    if len(sys.argv) > 1 and sys.argv[1] == "test":
      test.main()

    elif len(sys.argv) > 1 and sys.argv[1] == "single":
      queue = DummyQueue()
      db = Database()
      queue.add(" ".join(sys.argv[2:]))
      htmldoc = queue.get()
      digimon = Digimon(htmldoc, queue.add, db.add)
      print(digimon)

    else:
      main()