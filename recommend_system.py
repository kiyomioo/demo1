from build_kg import *
from word_extract import *
from kg_parser import *

class Recommend:
    def __init__(self):
        self.kg = build_kg()
        self.extractor = word_extract()
        self.parser = kg_parser()

    def recommend_sys(self,text):
        self.kg.export_data()
        self.kg.create_graph_nodes()
        self.kg.create_graph_rels()
        extracted_keywords = self.extractor.extract_keywords(text)
        print(extracted_keywords)
        self.parser.parser_main(extracted_keywords)


if __name__ == '__main__':

    handler = Recommend()
    # 输入一段话
    title = "运维工程师"
    education = "大专"
    salary = "2-4K"
    description = "我擅长项目技术方案的编写与编程,学习能力很强"
    text = title + education + salary + description
    handler.recommend_sys(text)