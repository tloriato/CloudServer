import itertools  
from bs4 import BeautifulSoup

class Digimon:
    def __init__(self, htmldoc, add_to_queue = None, add_to_database = None):
        self.__table_trs = BeautifulSoup(htmldoc, "html.parser").table.find_all('tr')
        
        print(f'Parsing...')

        try:

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
        
        except:
            try:
                print(f'Letting it go... {self.name}')
            except:
                print(f'Letting it go!')
    
 

    def __str__(self):
        return f"Name: {self.name} \nLevel: {self.level} \nType: {self.type} \nAttribute: {self.attribute} \nFamily: {self.family} \nPrior Forms: {self.prior_forms} \nNext Forms: {self.next_forms} \nVariations: {self.variations}"

    def get_name(self):
        return self.name

    def __get_name(self):
        return self.__table_trs[0].td.span.b.string.strip()

    def __get_level(self, row):
        level = row.contents[3].text.strip()
        if (level.find('[') > 0):
            return level[:level.find('[')]
        return level

    def __get_type(self, row):
      text = row.contents[3].text
      #TODO: Deal with this when persisting data
      if (text.find("(Ja:)") > 0 or text.find("(En:)") > 0):
          types = []
          types.append(text[text.find("("):text.find("(", text.find(")"))].strip())
          types.append(text[text.find("(", text.find("(") + 1):].strip())
          return types
      return [text.strip()]

    def __get_attribute(self, row):
        attributes = []
        for attribute in row.contents[3].stripped_strings:
            attributes.append(attribute)
        return attributes
    
    def __get_family(self, row):
      family = []
      for children in row.contents[3].stripped_strings:
          family.append(children)
      return family
    
    def __get_prior_forms(self, row):
        prior_forms = []
        table_element = row.contents[3].a

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
        table_element = row.contents[3].a

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