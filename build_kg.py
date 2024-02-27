
import csv
import os
from py2neo import Node, Graph, Relationship, NodeMatcher,Transaction
import pandas as pd
import json
import re
from tqdm import tqdm


class BuildKG:
    def __init__(self):
        print("初始化...")
        # 初始化图数据库连接
        self.graph = Graph("http://localhost:7474", user="neo4j", password="123456")
        # 获取当前目录
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        # 数据路径
        self.data_path = os.path.join(cur_dir, 'data/data1.csv')

    def preprocess_data(self):

        # 打开输出文件
        out_company = open("data/out_company.csv", "w", encoding='utf-8')
        out_company.write("company,address\n")

        out_job_position = open("data/out_title.csv", "w", encoding='utf-8')
        out_job_position.write("company,title,link\n")

        out_salary_requirements = open("data/out_salary.csv", "w", encoding='utf-8')
        out_salary_requirements.write("company,title,salary\n")

        out_education_requirements = open("data/out_education.csv", "w", encoding='utf-8')
        out_education_requirements.write("company,title,education\n")

        out_recruiters = open("data/out_hiring_manager.csv", "w", encoding='utf-8')
        out_recruiters.write("company,title,hiring_manager,last_active\n")

        # 读取原始数据
        df = pd.read_csv(self.data_path)
        for _, row in df.iterrows():
            company = row["company"]
            address = row["address"]
            out_company.write(f"{company},{address}\n")

            # 处理链接信息
            link = row["link"]
            out_job_position.write(f"{company},{row['title']},{link}\n")

            # 处理招聘者信息
            hiring_manager = row["hiring_manager"]
            last_active = row["last_active"]
            out_recruiters.write(f"{company},{row['title']},{hiring_manager},{last_active}\n")

            # 处理薪资要求信息
            salary = row["salary"]
            out_salary_requirements.write(f"{company},{row['title']},{salary}\n")

            # 处理学历要求信息
            education = row["education"]
            out_education_requirements.write(f"{company},{row['title']},{education}\n")

        # 关闭文件
        out_company.close()
        out_job_position.close()
        out_salary_requirements.close()
        out_education_requirements.close()
        out_recruiters.close()

    def load_data(self):
        print("加载数据集...")
        tx = self.graph.begin()
        try:
            tx.run("MATCH ()-[r]->() DELETE r")
            tx.run("MATCH (r) DELETE r")

            print("Loading companies...")
            tx.run("""
                LOAD CSV WITH HEADERS FROM "file:///data/out_company.csv" AS row
                MERGE (c:Company {name: row.company, address: row.address})
            """)

            print("Loading job positions...")
            tx.run("""
                LOAD CSV WITH HEADERS FROM "file:///data/out_title.csv" AS row
                MERGE (j:JobPosition {title: row.title, link: row.link})
                WITH row.company AS company_name, j
                MATCH (c:Company {name: company_name})
                MERGE (c)-[:OFFERS]->(j)
            """)

            print("Loading salary requirements...")
            tx.run("""
                LOAD CSV WITH HEADERS FROM "file:///data/out_salary.csv" AS row
                MATCH (c:Company {name: row.company})
                MATCH (j:JobPosition {title: row.title})
                MERGE (c)-[:HAS_SALARY_REQUIREMENT]->(j)
                SET j.salary = row.salary
            """)

            print("Loading education requirements...")
            tx.run("""
                LOAD CSV WITH HEADERS FROM "file:///data/out_education.csv" AS row
                MATCH (c:Company {name: row.company})
                MATCH (j:JobPosition {title: row.title})
                MERGE (c)-[:HAS_EDUCATION_REQUIREMENT]->(j)
                SET j.education = row.education
            """)

            print("Loading recruiters...")
            tx.run("""
                LOAD CSV WITH HEADERS FROM "file:///data/out_hiring_manager.csv" AS row
                MATCH (c:Company {name: row.company})
                MATCH (j:JobPosition {title: row.title})
                MERGE (h:Recruiter {name: row.hiring_manager})
                MERGE (c)-[:HAS_RECRUITER]->(h)
                SET h.last_active = row.last_active
            """)
            self.graph.commit(tx)
        except:
            tx.rollback()
            raise

class BuildKG1:
    def __init__(self):
        self.graph = Graph("http://localhost:7474", user="neo4j", password="123456")

    def create_or_update_company(self, tx, name, address):
        result = tx.run("MATCH (c:Company {name: $name}) RETURN c", name=name)
        existing_node = result.data()
        if existing_node:
            tx.run("MATCH (c:Company {name: $name}) DETACH DELETE c", name=name)
        tx.run("CREATE (c:Company {name: $name, address: $address})", name=name, address=address)

    def create_or_update_job_position(self, tx, company_name, title, link):
        result = tx.run("MATCH (j:JobPosition {title: $title}) RETURN j", title=title)
        existing_node = result.data()
        if existing_node:
            tx.run("MATCH (j:JobPosition {title: $title}) DETACH DELETE j", title=title)
        tx.run("""
            MATCH (c:Company {name: $company_name})
            CREATE (c)-[:OFFERS]->(j:JobPosition {title: $title, link: $link})
        """, company_name=company_name, title=title, link=link)

    def create_or_update_recruiter(self, tx, company_name, title, hiring_manager, last_active):
        tx.run("""
            MATCH (c:Company {name: $company_name})
            CREATE (c)-[:CONTACTS]->(r:Recruiter {title: $title, name: $hiring_manager, last_active: $last_active})
        """, company_name=company_name, title=title, hiring_manager=hiring_manager, last_active=last_active)

    def create_relationship_salary_requirement(self, tx, company_name, title, salary):
        tx.run("""
            MATCH (c:Company {name: $company_name})
            MATCH (j:JobPosition {title: $title})
            MERGE (c)-[:OFFERS_SALARY]->(j)
            SET j.salary = $salary
        """, company_name=company_name, title=title, salary=salary)

    def create_relationship_education_requirement(self, tx, company_name, title, education):
        tx.run("""
            MATCH (c:Company {name: $company_name})
            MATCH (j:JobPosition {title: $title})
            MERGE (c)-[:OFFERS_EDUCATION]->(j)
            SET j.education = $education
        """, company_name=company_name, title=title, education=education)

    def load_data(self):

        print("加载数据集...")
        tx = self.graph.begin()
        try:
            tx.run("MATCH ()-[r]->() DELETE r")
            tx.run("MATCH (r) DELETE r")

            with open("data/out_company.csv", "r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.create_or_update_company(tx, row["company"], row["address"])

            with open("data/out_title.csv", "r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    company_name = row.get("company", None)
                    title = row.get("title", None)
                    link = row.get("link", None)
                    if company_name and title and link:
                        self.create_or_update_job_position(tx, company_name, title, link)

            with open("data/out_salary.csv", "r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    company_name = row.get("company", None)
                    title = row.get("title", None)
                    salary = row.get("salary", None)
                    if company_name and title and salary:
                        self.create_relationship_salary_requirement(tx, company_name, title, salary)

            with open("data/out_education.csv", "r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    company_name = row.get("company", None)
                    title = row.get("title", None)
                    education = row.get("education", None)
                    if company_name and title and education:
                        self.create_relationship_education_requirement(tx, company_name, title, education)

            with open("data/out_hiring_manager.csv", "r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.create_or_update_recruiter(tx, row["company"], row["title"], row["hiring_manager"], row["last_active"])

            self.graph.commit(tx)
        except:
            tx.rollback()
            raise


if __name__ == '__main__':
    kg = BuildKG1()
    kg.load_data()
