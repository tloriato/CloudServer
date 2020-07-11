#!/usr/bin/env python3
import sys
import os
import requests
from ratelimit import limits, sleep_and_retry
import queue as Queue
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
dot_env = os.environ

"""
Module Docstring
"""

__author__ = "Tiago Loriato"
__version__ = "0.1.0"
__license__ = "GNU GPLv3"

class CrawlerQueue:  
    def __init__(self):
        self.__cache = {}
        self.__queue = Queue.Queue(maxsize=0)
    
    @sleep_and_retry
    @limits(calls=10, period=60*60)
    def __get_page_content(self, name):
      base_url = "https://digimon.fandom.com/wiki/"
      response = requests.get(base_url + name, headers={'Accept-Encoding': 'identity'})
      if response.status_code != 200:
          raise Exception("Failed Call")
      else:
        return response.content
    
    def get(self):
        try:
            digimon_name = self.__queue.get_nowait()
        except Queue.Empty:
            return None
        try:
            htmldoc = self.__get_page_content(digimon_name)
            return htmldoc
        except:
            self.__queue.put(digimon_name)
            return None

    def add(self, name):
        if self.__cache.get(name, None) is None:
            self.__cache[name] = 1
            self.__queue.put(name)
            return True
        return False
    
class Digimon:
    def __init__(self, htmldoc):
        self.__table_trs = BeautifulSoup(htmldoc, "html.parser").table.find_all('tr')

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

    def __str__(self):
      return f"Name: {self.name} \nLevel: {self.level} \nType: {self.type} \nAttribute: {self.attribute} \nFamily: {self.family} \nPrior Forms: {self.prior_forms} \nNext Forms: {self.next_forms} \nVariations: {self.variations}"

    def __get_name(self):
        return self.__table_trs[0].td.span.b.string.strip()

    def __get_level(self, row):
        return row.contents[2].string.strip()

    def __get_type(self, row):
        return row.contents[2].string.strip()

    def __get_attribute(self, row):
        return row.contents[2].string.strip()
    
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
        
def database_session():
    from neo4j import GraphDatabase, basic_auth
    driver = GraphDatabase.driver(dot_env["NEO4J_BOLT_IP"], auth=basic_auth(dot_env["NEO4J_USER"], dot_env["NEO4J_PASSWORD"]))
    return driver.session()

def main():
    """ Main entry point of the app """
    #session = database_session()
    #print(get_digimon_info("https://digimon.fandom.com/wiki/Agumon"))

def test():

    with open("agumon.html") as htmldoc:
        Agumon = Digimon(htmldoc)
        assert Agumon.name == "Agumon"
        assert Agumon.level == "Rookie"
        assert Agumon.type == "Reptile"
        assert Agumon.attribute == "Vaccine"
        assert Agumon.family == ["Nature Spirits", "Virus Busters", "Metal Empire", "Unknown", "Dragon's Roar"]
        assert Agumon.prior_forms == ["Koromon"] 
        assert Agumon.next_forms == ["Greymon", "Centarumon", "Meramon", "BlackAgumon", "Agumon -Yuki's Kizuna-"]
        assert Agumon.variations == ['Agumon (2006 anime)', 'Agumon X', 'BlackAgumon', 'SnowAgumon', 'DotAgumon', 'Agumon Expert', 'Fake Agumon Expert', 'SantaAgumon', 'BushiAgumon', 'BlackAgumon X']

if __name__ == "__main__":
    """ This is executed when run from the command line """
    if sys.argv[1] == "test":
      test()
    else:
      main()