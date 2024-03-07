from py2neo import Graph
from word_extract import *

class kg_parser:
    def __init__(self):
        self.kg = Graph("http://localhost:7474", user="neo4j", password="123456")

    def parser_main(self,entity_dict):

        query = "MATCH (c:Company)-[:offer]->(t:Title) WHERE"

        # 用于跟踪是否添加了任何条件
        added_condition = False

        # 遍历实体类型和实体词典
        for entity, entity_type_list in entity_dict.items():
            entity_type = entity_type_list[0]
            if entity_type == 'salary':
                if added_condition:
                    query += " AND"
                query += " EXISTS ((c)-[:pay]->(:Salary {name: '%s'}))" % entity
                added_condition = True
            elif entity_type == 'keyword':
                if added_condition:
                    query += " AND"
                query += " EXISTS ((c)-[:require_ability]->(:Keyword {name: '%s'}))" % entity
                added_condition = True
            # 可以根据其他实体类型的需要继续添加逻辑
            elif entity_type == 'education':
                if added_condition:
                    query += " AND"
                query += " EXISTS ((c)-[:require_edu]->(:Education {name: '%s'}))" % entity
                added_condition = True
            elif entity_type == 'title':
                if added_condition:
                    query += " AND"
                query += " t.name = '%s'" % entity
                added_condition = True

        query += " RETURN c, t"

        res = self.kg.run(query).data()
        print(res)




