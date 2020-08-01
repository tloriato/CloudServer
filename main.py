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

load_dotenv(find_dotenv())
dot_env = os.environ

"""
Module Docstring
"""

__author__ = "Tiago Loriato"
__version__ = "0.1.0"
__license__ = "GNU GPLv3"

class Database():
    #TODO: Optimize the hell out of this later
    def __init__(self):
        self.session = self.__database_session()

    def __database_session(self):
        driver = GraphDatabase.driver(dot_env["NEO4J_BOLT_IP"], auth=basic_auth(dot_env["NEO4J_USER"], dot_env["NEO4J_PASSWORD"]))
        return driver.session()
    
    def __get_node(self, node_type, name):
        query = "MATCH (n:$node_type) WHERE n.name = $name RETURN n.name"
        return self.session.run(query, node_type=node_type, name=name).single()

    def __create_node(self, node_type, name):
        return self.session.run("CREATE (n:$type {name: $name}) RETURN d.name", type=node_type, name=name).single()

    def __create_relationship(self,origin_name, origin_type, relationship_type, destination_name, destination_type):
        return self.session.run("MATCH (n:$origin_type) WHERE n.name = $origin_name MATCH (d:$destination_type) WHERE d.name = $destination_name MERGE (n)-[:$relationship_type]->(d)", relationship_type=relationship_type, origin_name=origin_name, origin_type=origin_type, destination_name=destination_name, destination_type=destination_type)

    def __get_or_create(self, node_type, name):
        result = self.__get_node(node_type, name)
        if result is None:
            return self.__create_node(node_type, name)
        return result

    def add(self, digimon):
        main_node = self.__get_or_create("Digimon", digimon.name)

        # TODO: Deal with EN vs JP versions
        for evolution in digimon.next_forms:
            next_form = self.__get_or_create("Digimon", evolution)
            self.__create_relationship(main_node, "Digimon", "EVOLTES_TO", next_form, "Digimon")

        for attribute in digimon.attribute:
            attribute_name = self.__get_or_create("Attribute", attribute)
            self.__create_relationship(main_node, "Digimon", "HAS_ATTRIBUTE", attribute_name, "Attribute")
        
        for family in digimon.family:
            family_name = self.__get_or_create("Family", family)
            self.__create_relationship(main_node, "Digimon", "BELONGS_TO_FAMILY", family_name, "Family")
        
        for children in digimon.prior_forms:
            children = self.__get_or_create("Digimon", children)
            self.__create_relationship(children, "Digimon", "EVOLTES_TO", main_node, "Digimon")

        for variation in digimon.variations:
            other = self.__get_or_create("Digimon", variation)
            self.__create_relationship(main_node, "Digimon", "IS_VARIATION", other, "Digimon")


class DummyQueue:
    def __init__(self):
        self.__cache = {}
        self.__queue = []

    @sleep_and_retry
    @limits(calls=600000000000, period=60*60)
    def __crawler(self, name):
      base_url = "https://digimon.fandom.com/wiki/"
      response = requests.get(base_url + name, headers={'Accept-Encoding': 'identity'})
      if response.status_code != 200:
          raise Exception("Failed Call")
      else:
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
            return self.__crawler(digimon)
        return None

class Digimon:
    def __init__(self, htmldoc, add_to_queue = None, add_to_database = None):
        self.__table_trs = BeautifulSoup(htmldoc, "html.parser").table.find_all('tr')
        
        print(f'Parsing...')

    
        self.name = self.__get_name()

        self.level = []
        self.attribute = []
        self.type = []
        self.family = []
        self.prior_forms = []
        self.next_forms = []
        self.variations = []
    
    
        for row in self.__table_trs:
            if row.find(text="Level"):
                self.level = self.__get_level(row)
            elif row.find(text="Attribute"):
                self.attribute = self.__get_attribute(row)
            elif row.find(text="Type"):
                self.type = self.__get_type(row)
            elif row.find(text="Family"):
                self.family = self.__get_family(row)
            elif row.find(text="Prior forms"):
                self.prior_forms = self.__get_prior_forms(row)
            elif row.find(text="Next forms"):
                self.next_forms = self.__get_next_forms(row)
            elif row.find(text="Variations"):
                # TODO: Fix this. We call this function twice because of the "expand" element
                # when it should be called just once. Right now we are ignoring the error on the second call inside
                # the function and leaving the instance variable untouched
                self.__set_variations(row)
            else:
                pass

        if add_to_queue is not None:
            for (a, b, c) in itertools.zip_longest(self.next_forms, self.prior_forms, self.variations):
                if a is not None:
                    add_to_queue(a)
                if b is not None:
                    add_to_queue(b)
                if c is not None:
                    add_to_queue(c)

        if add_to_database is not None:
            add_to_database(self)
        
        print(f'Finished {self.name}')
    
 

    def __str__(self):
        return f"Name: {self.name} \nLevel: {self.level} \nType: {self.type} \nAttribute: {self.attribute} \nFamily: {self.family} \nPrior Forms: {self.prior_forms} \nNext Forms: {self.next_forms} \nVariations: {self.variations}"

    def get_name(self):
        return self.name

    def __get_name(self):
        return self.__table_trs[0].td.span.b.string.strip()

    def __get_level(self, row):
        level = row.contents[2].text.strip()
        if (level.find('[') > 0):
            return level[:level.find('[')]
        return level

    def __get_type(self, row):
      text = row.contents[2].text
      #TODO: Deal with this when persisting data
      if (text.find("(Ja:)") > 0 or text.find("(En:)") > 0):
          types = []
          types.append(text[text.find("("):text.find("(", text.find(")"))].strip())
          types.append(text[text.find("(", text.find("(") + 1):].strip())
          return types
      return [text.strip()]

    def __get_attribute(self, row):
        attributes = []
        for attribute in row.contents[2].stripped_strings:
            attributes.append(attribute)
        return attributes
    
    def __get_family(self, row):
      family = []
      for children in row.contents[2].stripped_strings:
          family.append(children)
      return family
    
    def __get_prior_forms(self, row):
        prior_forms = []
        table_element = row.contents[2].a

        while table_element is not None:
          try:
              if table_element.has_attr("title"):
                prior_forms.append(table_element.get_text())
          except AttributeError:
              pass
          table_element = table_element.next_sibling

        return prior_forms

    def __get_next_forms(self, row):
        next_forms = []
        table_element = row.contents[2].a

        while table_element is not None:
          try:
              if table_element.has_attr("title"):
                next_forms.append(table_element.get_text())
          except AttributeError:
              pass
          table_element = table_element.next_sibling

        return next_forms
      
    def __set_variations(self, row):
        try:
            variations = []
            table_element = row.contents[1].table.contents[3].a
            while table_element is not None:
              try:
                  if table_element.has_attr("title"):
                    variations.append(table_element.get_text())
              except AttributeError:
                  pass
              table_element = table_element.next_sibling
            self.variations = variations
        except Exception:
            pass
        
def main():
    """ Main entry point of the app """
    queue = DummyQueue()
    db = Database()
    queue.add('Agumon')

    htmldoc = queue.get()

    f = open("results.txt", "w")

    while(htmldoc is not None):
        digimon = Digimon(htmldoc, queue.add, db.add)
        htmldoc = queue.get()

def test():

    with open("agumon.html") as htmldoc:
        Agumon = Digimon(htmldoc)
        assert Agumon.name == "Agumon"
        assert Agumon.level == "Rookie"
        assert Agumon.type == ["Reptile"]
        assert Agumon.attribute == ["Vaccine"]
        assert Agumon.family == ["Nature Spirits", "Virus Busters", "Metal Empire", "Unknown", "Dragon's Roar"]
        assert Agumon.prior_forms == ["Koromon"] 
        assert Agumon.next_forms == ["Greymon", "Centarumon", "Meramon", "BlackAgumon", "Agumon -Yuki's Kizuna-"]
        assert Agumon.variations == ['Agumon (2006 anime)', 'Agumon X', 'BlackAgumon', 'SnowAgumon', 'DotAgumon', 'Agumon Expert', 'Fake Agumon Expert', 'SantaAgumon', 'BushiAgumon', 'BlackAgumon X']

if __name__ == "__main__":
    """ This is executed when run from the command line """

    if len(sys.argv) > 1 and sys.argv[1] == "test":
      test()

    elif len(sys.argv) > 1 and sys.argv[1] == "single":
      queue = DummyQueue()
      db = Database()
      queue.add(" ".join(sys.argv[2:]))
      htmldoc = queue.get()
      digimon = Digimon(htmldoc, queue.add, db.add)
      print(digimon)

    else:
      main()